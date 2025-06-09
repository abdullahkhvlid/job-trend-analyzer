import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import time
import random
from datetime import datetime, timedelta
import re
from urllib.parse import urljoin, quote_plus
import logging
from typing import List, Dict, Optional
import csv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class JobScraper:
    def __init__(self):
        self.delay_range = (3, 6)
        self.jobs_data = []
        
    def random_delay(self, multiplier=1.0):
        """Add random delay between requests to be respectful"""
        delay = random.uniform(*self.delay_range) * multiplier
        logger.info(f"Waiting for {delay:.2f} seconds...")
        time.sleep(delay)

    def clean_text(self, text: str) -> str:
        """Clean and normalize text data"""
        if not text:
            return ""
        text = text.replace("\n", " ").replace("\t", " ")
        return " ".join(text.strip().split())

    def extract_skills(self, description: str) -> List[str]:
        """Extract skills from job description using keyword matching"""
        if not description:
            return []
        
        skill_keywords = [
            # Programming Languages
            'python', 'java', 'javascript', 'typescript', 'c#', 'c++', 'php', 'ruby', 'go', 'swift', 'kotlin', 'scala', 'rust', 'perl', 
            # Web Development (Frontend)
            'html', 'css', 'react', 'angular', 'vue', 'jquery', 'bootstrap', 'tailwind', 'sass', 'less', 'svelte',
            # Web Development (Backend)
            'nodejs', 'django', 'flask', 'spring', 'ruby on rails', '.net', 'laravel', 'express',
            # Databases
            'sql', 'mysql', 'postgresql', 'sqlite', 'mongodb', 'redis', 'cassandra', 'elasticsearch', 'dynamodb', 'oracle', 'sql server',
            # Cloud Platforms
            'aws', 'azure', 'gcp', 'google cloud', 'amazon web services', 'heroku', 'digitalocean', 'kubernetes', 'docker', 'terraform',
            # DevOps & Tools
            'git', 'github', 'gitlab', 'jenkins', 'ansible', 'puppet', 'chef', 'ci/cd', 'jira', 'linux', 'bash',
            # Data Science & ML
            'machine learning', 'data science', 'artificial intelligence', 'ai', 'deep learning', 'nlp', 
            'pandas', 'numpy', 'scipy', 'scikit-learn', 'tensorflow', 'pytorch', 'keras', 'spark', 'hadoop',
            # Mobile Development
            'ios', 'android', 'react native', 'flutter',
            # Other
            'api', 'rest', 'graphql', 'microservices', 'agile', 'scrum'
        ]
        
        description_lower = description.lower()
        found_skills = set()
        
        for skill in skill_keywords:
            pattern = r"\b" + re.escape(skill.lower()) + r"\b"
            if re.search(pattern, description_lower):
                # Proper capitalization
                if skill.upper() in ["AWS", "GCP", "AI", "HTML", "CSS", "SQL", "API", "CI/CD", "NLP"]:
                    found_skills.add(skill.upper())
                elif skill == "c#":
                    found_skills.add("C#")
                elif skill == "c++":
                    found_skills.add("C++")
                elif skill == "javascript":
                    found_skills.add("JavaScript")
                elif skill == "nodejs":
                    found_skills.add("Node.js")
                else:
                    found_skills.add(skill.title())

        return sorted(list(found_skills))

    def parse_date(self, date_str: str) -> str:
        """Parse and normalize date strings from various formats"""
        if not date_str:
            return datetime.now().strftime("%Y-%m-%d")
        
        date_str = date_str.lower().strip().replace("posted", "").replace("on", "").strip()
        now = datetime.now()

        try:
            # Handle relative dates
            if "just now" in date_str or "today" in date_str or "hour" in date_str or "minute" in date_str:
                return now.strftime("%Y-%m-%d")
            elif "yesterday" in date_str:
                return (now - timedelta(days=1)).strftime("%Y-%m-%d")
            elif "day" in date_str:
                days_match = re.search(r"(\d+)", date_str)
                if days_match:
                    return (now - timedelta(days=int(days_match.group(1)))).strftime("%Y-%m-%d")
            elif "week" in date_str:
                weeks_match = re.search(r"(\d+)", date_str)
                if weeks_match:
                    return (now - timedelta(weeks=int(weeks_match.group(1)))).strftime("%Y-%m-%d")
            elif "month" in date_str:
                months_match = re.search(r"(\d+)", date_str)
                if months_match:
                    return (now - timedelta(days=int(months_match.group(1)) * 30)).strftime("%Y-%m-%d")
            
            # Handle ISO dates
            try:
                if 'T' in date_str or '+' in date_str:
                    parsed_date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                    return parsed_date.strftime("%Y-%m-%d")
            except ValueError:
                pass

            logger.warning(f"Could not parse date string: '{date_str}'. Defaulting to today.")
            return now.strftime("%Y-%m-%d")
            
        except Exception as e:
            logger.error(f"Date parsing failed for '{date_str}': {e}")
            return now.strftime("%Y-%m-%d")

    def scrape_linkedin_jobs(self, query: str = "software engineer", location: str = "United States", max_jobs: int = 50) -> List[Dict]:
        """Scrape jobs from LinkedIn using requests (public job listings)"""
        logger.info(f"Scraping LinkedIn for '{query}' in '{location}' (max_jobs={max_jobs})")
        jobs = []
        session = requests.Session()
        
        try:
            # Set up session with headers
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
            session.headers.update(headers)
            
            # LinkedIn job search URL (public listings)
            start = 0
            page_size = 25
            pages_to_scrape = min(3, (max_jobs // page_size) + 1)
            
            for page in range(pages_to_scrape):
                if len(jobs) >= max_jobs:
                    break
                    
                # LinkedIn public job search URL
                url = f"https://www.linkedin.com/jobs/search?keywords={quote_plus(query)}&location={quote_plus(location)}&start={start}&f_TPR=r604800"
                logger.info(f"Fetching LinkedIn page {page + 1}: {url}")
                
                response = session.get(url, timeout=30)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Find job cards
                job_cards = soup.find_all('div', class_='base-card')
                logger.info(f"Found {len(job_cards)} job cards on page {page + 1}")
                
                if not job_cards:
                    logger.warning("No job cards found. LinkedIn structure may have changed.")
                    break
                
                for card in job_cards:
                    if len(jobs) >= max_jobs:
                        break
                        
                    try:
                        # Extract job title
                        title_elem = card.find('h3', class_='base-search-card__title')
                        title = self.clean_text(title_elem.get_text() if title_elem else "")
                        
                        # Extract company name
                        company_elem = card.find('h4', class_='base-search-card__subtitle')
                        if not company_elem:
                            company_elem = card.find('a', {'data-tracking-control-name': 'public_jobs_topcard-org-name'})
                        company = self.clean_text(company_elem.get_text() if company_elem else "")
                        
                        # Extract location
                        location_elem = card.find('span', class_='job-search-card__location')
                        job_location = self.clean_text(location_elem.get_text() if location_elem else location)
                        
                        # Extract date
                        date_elem = card.find('time', class_='job-search-card__listdate')
                        if not date_elem:
                            date_elem = card.find('time')
                        date_posted = ""
                        if date_elem:
                            date_posted = self.parse_date(date_elem.get('datetime', '') or date_elem.get_text())
                        else:
                            date_posted = self.parse_date("")
                        
                        # Extract job link for description
                        link_elem = card.find('a', class_='base-card__full-link')
                        job_url = ""
                        if link_elem and link_elem.get('href'):
                            job_url = urljoin(url, link_elem['href'])
                        
                        # Basic description (we'll enhance this)
                        description = f"LinkedIn job posting for {title} at {company}. Location: {job_location}."
                        
                        # Try to get job snippet if available
                        snippet_elem = card.find('p', class_='job-search-card__snippet')
                        if snippet_elem:
                            description += " " + self.clean_text(snippet_elem.get_text())
                        
                        # Extract skills from description
                        skills = self.extract_skills(description)
                        
                        if title and company:
                            job = {
                                'title': title,
                                'company': company,
                                'location': job_location,
                                'date_posted': date_posted,
                                'skills': skills,
                                'source': 'LinkedIn',
                                'description': description[:500] + "..." if len(description) > 500 else description,
                                'url': job_url
                            }
                            jobs.append(job)
                            logger.debug(f"Added job: {title} at {company}")
                    
                    except Exception as e:
                        logger.warning(f"Error parsing LinkedIn job card: {e}")
                        continue
                
                start += page_size
                if page < pages_to_scrape - 1:  # Don't delay after last page
                    self.random_delay(0.8)
                    
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching LinkedIn data: {e}")
        except Exception as e:
            logger.error(f"Error scraping LinkedIn: {e}")
        finally:
            session.close()
        
        logger.info(f"Successfully scraped {len(jobs)} jobs from LinkedIn")
        return jobs

    def scrape_remoteok(self, query: str = "software", max_jobs: int = 50) -> List[Dict]:
        """Scrape jobs from RemoteOK"""
        logger.info(f"Scraping RemoteOK for '{query}' (max_jobs={max_jobs})")
        jobs = []
        session = requests.Session()
        
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Referer': 'https://remoteok.com/',
            }
            session.headers.update(headers)
            
            # Try multiple RemoteOK URLs
            search_term = query.lower().replace(' ', '-')
            urls_to_try = [
                f"https://remoteok.com/remote-{search_term}-jobs",
                f"https://remoteok.com/remote-dev-jobs",
                "https://remoteok.com/"
            ]
            
            for url in urls_to_try:
                logger.info(f"Trying RemoteOK URL: {url}")
                
                try:
                    response = session.get(url, timeout=30)
                    response.raise_for_status()
                    
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    # Find job rows
                    job_rows = soup.select('tr.job')
                    logger.info(f"Found {len(job_rows)} job rows on RemoteOK")
                    
                    if not job_rows:
                        logger.warning(f"No job rows found at {url}")
                        continue
                    
                    count = 0
                    for row in job_rows:
                        if count >= max_jobs:
                            break
                            
                        try:
                            # Extract title
                            title_elem = row.select_one('td.company h2')
                            if not title_elem:
                                title_elem = row.select_one('h2[itemprop="title"]')
                            title = self.clean_text(title_elem.get_text() if title_elem else "")
                            
                            # Extract company
                            company_elem = row.select_one('td.company h3')
                            if not company_elem:
                                company_elem = row.select_one('h3[itemprop="name"]')
                            company = self.clean_text(company_elem.get_text() if company_elem else "")
                            
                            # Location is typically "Remote"
                            location = "Remote"
                            
                            # Extract skills from tags
                            skill_tags = row.select('td.tags a.tag h3, td.tags span.tag h3')
                            skills = []
                            for tag in skill_tags:
                                skill_text = self.clean_text(tag.get_text())
                                if skill_text and not skill_text.startswith('$'):
                                    skills.append(skill_text)
                            
                            # Extract date
                            date_elem = row.select_one('td.time time')
                            date_posted = ""
                            if date_elem:
                                datetime_attr = date_elem.get('datetime', '')
                                if datetime_attr:
                                    try:
                                        parsed_date = datetime.fromisoformat(datetime_attr.replace('Z', '+00:00'))
                                        date_posted = parsed_date.strftime('%Y-%m-%d')
                                    except:
                                        date_posted = self.parse_date(date_elem.get_text())
                                else:
                                    date_posted = self.parse_date(date_elem.get_text())
                            else:
                                date_posted = self.parse_date("")
                            
                            # Create description
                            description = f"Remote {title} position at {company}."
                            if skills:
                                description += f" Required skills: {', '.join(skills[:5])}."
                            
                            # Extract additional skills from description
                            all_skills = self.extract_skills(description)
                            all_skills.extend([s for s in skills if s not in all_skills])
                            
                            if title and company:
                                job = {
                                    'title': title,
                                    'company': company,
                                    'location': location,
                                    'date_posted': date_posted,
                                    'skills': sorted(list(set(all_skills))),
                                    'source': 'RemoteOK',
                                    'description': description
                                }
                                jobs.append(job)
                                count += 1
                                logger.debug(f"Added RemoteOK job: {title} at {company}")
                        
                        except Exception as e:
                            logger.warning(f"Error parsing RemoteOK job row: {e}")
                            continue
                    
                    if jobs:  # If we found jobs, break out of URL loop
                        break
                        
                except requests.exceptions.RequestException as e:
                    logger.error(f"Error fetching RemoteOK URL {url}: {e}")
                    continue
            
            self.random_delay(0.5)
            
        except Exception as e:
            logger.error(f"Error scraping RemoteOK: {e}")
        finally:
            session.close()
        
        logger.info(f"Successfully scraped {len(jobs)} jobs from RemoteOK")
        return jobs

    def scrape_glassdoor_demo(self, query: str = "software engineer", location: str = "United States", count: int = 15) -> List[Dict]:
        """Demo Glassdoor data (as in original code)"""
        logger.info(f"Generating demo Glassdoor data for '{query}' in '{location}'")
        demo_companies = ["Google", "Microsoft", "Amazon", "Apple", "Meta", "Netflix", "Tesla", "Uber", "Airbnb", "Spotify"]
        demo_titles = ["Software Engineer", "Senior Software Engineer", "Data Scientist", "Product Manager", "DevOps Engineer", "Full Stack Developer"]
        demo_locations = ["San Francisco, CA", "Seattle, WA", "New York, NY", "Austin, TX", "Boston, MA", "Remote"]
        demo_skills = [
            ["Python", "Django", "PostgreSQL", "AWS"],
            ["JavaScript", "React", "Node.js", "MongoDB"],
            ["Java", "Spring", "MySQL", "Docker"],
            ["Python", "Machine Learning", "TensorFlow", "SQL"],
            ["Go", "Kubernetes", "Docker", "AWS"]
        ]
        
        jobs = []
        for i in range(min(count, 30)):
            job = {
                'title': random.choice(demo_titles),
                'company': random.choice(demo_companies),
                'location': random.choice(demo_locations),
                'skills': random.choice(demo_skills),
                'date_posted': (datetime.now() - timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%d'),
                'source': 'Glassdoor (Demo)',
                'description': "Demo job description for testing purposes."
            }
            jobs.append(job)
        return jobs

    def scrape_all_platforms(self, query: str = "software engineer", location: str = "United States") -> List[Dict]:
        """Scrape jobs from all platforms with focus on LinkedIn and RemoteOK"""
        logger.info(f"Starting job scraping for '{query}' in '{location}'...")
        all_jobs = []
        
        # Scrape LinkedIn
        try:
            logger.info("--- Starting LinkedIn Scrape ---")
            linkedin_jobs = self.scrape_linkedin_jobs(query, location, max_jobs=50)
            all_jobs.extend(linkedin_jobs)
            logger.info(f"--- Finished LinkedIn Scrape ({len(linkedin_jobs)} jobs) ---")
        except Exception as e:
            logger.error(f"LinkedIn scraping failed: {e}")
        
        self.random_delay()
        
        # Scrape RemoteOK
        try:
            logger.info("--- Starting RemoteOK Scrape ---")
            remote_query = query.split()[0] if query else "software"
            remote_jobs = self.scrape_remoteok(remote_query, max_jobs=50)
            all_jobs.extend(remote_jobs)
            logger.info(f"--- Finished RemoteOK Scrape ({len(remote_jobs)} jobs) ---")
        except Exception as e:
            logger.error(f"RemoteOK scraping failed: {e}")
        
        self.random_delay()
        
        # Add demo Glassdoor data
        try:
            logger.info("--- Adding Demo Glassdoor Data ---")
            glassdoor_jobs = self.scrape_glassdoor_demo(query, location, count=10)
            all_jobs.extend(glassdoor_jobs)
            logger.info(f"--- Added Demo Glassdoor Data ({len(glassdoor_jobs)} jobs) ---")
        except Exception as e:
            logger.error(f"Glassdoor demo failed: {e}")
        
        # Deduplication
        logger.info(f"Total jobs before deduplication: {len(all_jobs)}")
        seen = set()
        unique_jobs = []
        
        for job in all_jobs:
            title_norm = self.clean_text(job.get('title', '')).lower()
            company_norm = self.clean_text(job.get('company', '')).lower()
            
            if not title_norm or not company_norm:
                continue
                
            key = (title_norm, company_norm)
            
            if key not in seen:
                seen.add(key)
                unique_jobs.append(job)
            else:
                logger.debug(f"Duplicate removed: {title_norm} at {company_norm}")
        
        logger.info(f"Total unique jobs scraped: {len(unique_jobs)}")
        self.jobs_data = unique_jobs
        return unique_jobs

    def save_to_csv(self, filename: str = 'jobs_data.csv'):
        """Save scraped data to CSV"""
        if not self.jobs_data:
            logger.warning("No data to save to CSV.")
            return
        
        logger.info(f"Saving {len(self.jobs_data)} jobs to {filename}")
        fieldnames = ['title', 'company', 'location', 'date_posted', 'skills', 'source', 'description']
        
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile, quoting=csv.QUOTE_ALL)
                writer.writerow(fieldnames)
                
                for job in self.jobs_data:
                    row = []
                    for field in fieldnames:
                        value = job.get(field, '')
                        if field == 'skills' and isinstance(value, list):
                            value = ', '.join(value)
                        row.append(str(value))
                    writer.writerow(row)
            
            logger.info(f"Data successfully saved to {filename}")
        except Exception as e:
            logger.error(f"Error saving data to CSV: {e}")

    def get_summary_stats(self) -> Dict:
        """Calculate summary statistics"""
        if not self.jobs_data:
            return {'total_jobs': 0, 'source_breakdown': {}, 'top_skills': [], 'date_range': {}}

        sources = [job.get('source', 'Unknown') for job in self.jobs_data]
        source_counts = {}
        for source in sources:
            source_counts[source] = source_counts.get(source, 0) + 1

        all_skills = []
        for job in self.jobs_data:
            skills = job.get('skills', [])
            if isinstance(skills, list):
                all_skills.extend(skills)
            elif isinstance(skills, str) and skills:
                all_skills.extend([s.strip() for s in skills.split(',')])

        skill_counts = {}
        for skill in all_skills:
            if skill:
                skill_counts[skill] = skill_counts.get(skill, 0) + 1
        
        top_skills = sorted(skill_counts.items(), key=lambda x: x[1], reverse=True)

        dates = [job.get('date_posted') for job in self.jobs_data if job.get('date_posted')]
        date_range = {
            'earliest': min(dates) if dates else None,
            'latest': max(dates) if dates else None
        }

        return {
            'total_jobs': len(self.jobs_data),
            'source_breakdown': source_counts,
            'top_skills': top_skills,
            'date_range': date_range
        }

def main():
    """Main function to run the scraper"""
    scraper = JobScraper()
    
    search_query = "software engineer"
    search_location = "United States"
    output_filename = 'linkedin_remoteok_jobs.csv'
    
    print("🚀 Starting Enhanced Job Scraper (LinkedIn + RemoteOK Focus)...")
    print(f"Search Query: {search_query}")
    print(f"Location: {search_location}")
    print("=" * 60)
    
    start_time = time.time()
    
    try:
        jobs = scraper.scrape_all_platforms(search_query, search_location)
        end_time = time.time()
        
        print(f"\nScraping completed in {end_time - start_time:.2f} seconds.")
        
        if jobs:
            print(f"\n✅ Successfully scraped {len(jobs)} unique jobs!")
            scraper.save_to_csv(output_filename)
            
            # Show summary statistics
            stats = scraper.get_summary_stats()
            print("\n📊 Summary Statistics:")
            print(f"  Total Jobs: {stats['total_jobs']}")
            print(f"  Sources: {stats['source_breakdown']}")
            
            if stats.get('top_skills'):
                top_5_skills = dict(stats['top_skills'][:5])
                print(f"  Top 5 Skills: {top_5_skills}")
            
            date_range = stats.get('date_range', {})
            if date_range.get('earliest') and date_range.get('latest'):
                print(f"  Date Range: {date_range['earliest']} to {date_range['latest']}")
            
            # Show sample jobs
            print(f"\n📝 Sample Jobs (first 5):")
            for i, job in enumerate(jobs[:5]):
                print(f"\n{i+1}. {job.get('title', 'N/A')} at {job.get('company', 'N/A')}")
                print(f"   Location: {job.get('location', 'N/A')}")
                
                skills = job.get('skills', [])
                if isinstance(skills, list):
                    skills_display = ", ".join(skills[:5])
                elif isinstance(skills, str):
                    skills_display = ", ".join([s.strip() for s in skills.split(",")[:5]])
                else:
                    skills_display = "N/A"
                    
                print(f"   Skills: {skills_display}")
                print(f"   Date: {job.get('date_posted', 'N/A')}")
                print(f"   Source: {job.get('source', 'N/A')}")
        else:
            print("\n❌ No jobs were scraped. Check the logs for errors.")
            
    except Exception as e:
        logger.error(f"Main execution failed: {e}", exc_info=True)
        print(f"\n❌ Scraping failed: {e}")

if __name__ == "__main__":
    main()