import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import re
from collections import Counter
import numpy as np

# Page configuration
st.set_page_config(
    page_title="Job Market Dashboard",
    page_icon="💼",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        text-align: center;
        color: #1f77b4;
        margin-bottom: 2rem;
    }
    .metric-container {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 10px;
        margin: 1rem 0;
    }
    .chart-container {
        background-color: white;
        padding: 1rem;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data
def load_data(file_path):
    """Load and process job data"""
    try:
        df = pd.read_csv(file_path)
        
        # Clean and process the data
        df['title'] = df['title'].astype(str).str.strip()
        df['company'] = df['company'].astype(str).str.strip()
        df['location'] = df['location'].astype(str).str.strip()
        df['source'] = df['source'].astype(str).str.strip()
        
        # Process skills column
        df['skills_list'] = df['skills'].apply(parse_skills)
        
        # Process date_posted
        df['date_posted'] = pd.to_datetime(df['date_posted'], errors='coerce')
        df = df.dropna(subset=['date_posted'])
        
        # Extract city from location
        df['city'] = df['location'].apply(extract_city)
        
        return df
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return None

def parse_skills(skills_str):
    """Parse skills string into a list"""
    if pd.isna(skills_str) or skills_str == '' or skills_str == 'nan':
        return []
    
    # Split by comma and clean each skill
    skills = [skill.strip() for skill in str(skills_str).split(',')]
    return [skill for skill in skills if skill and skill != 'nan']

def extract_city(location_str):
    """Extract city name from location string"""
    if pd.isna(location_str) or location_str == '' or location_str == 'nan':
        return 'Unknown'
    
    location_str = str(location_str).strip()
    
    # Handle common patterns
    if location_str.lower() == 'remote':
        return 'Remote'
    
    # Extract city from "City, State" or "City, Country" format
    if ',' in location_str:
        city = location_str.split(',')[0].strip()
        return city if city else 'Unknown'
    
    return location_str

def get_top_job_titles(df, n=5):
    """Get top N most in-demand job titles"""
    title_counts = df['title'].value_counts().head(n)
    return title_counts

def get_top_skills(df, n=10):
    """Get top N most frequent skills"""
    all_skills = []
    for skills_list in df['skills_list']:
        all_skills.extend(skills_list)
    
    skill_counts = Counter(all_skills)
    return dict(skill_counts.most_common(n))

def get_top_cities(df, n=10):
    """Get cities with highest number of job openings"""
    city_counts = df['city'].value_counts().head(n)
    return city_counts

def create_posting_trends(df):
    """Create posting trends over time"""
    # Group by date and count jobs
    daily_counts = df.groupby(df['date_posted'].dt.date).size().reset_index()
    daily_counts.columns = ['date', 'job_count']
    daily_counts['date'] = pd.to_datetime(daily_counts['date'])
    
    return daily_counts

def main():
    # Header
    st.markdown('<div class="main-header">💼 Job Market Dashboard</div>', unsafe_allow_html=True)
    
    # Sidebar
    st.sidebar.header("📊 Dashboard Controls")
    
    # File upload
    uploaded_file = st.sidebar.file_uploader(
        "Upload CSV file",
        type=['csv'],
        help="Upload your job data CSV file"
    )
    
    # Default file option
    use_default = st.sidebar.checkbox("Use sample data", value=True)
    
    # Load data
    df = None
    if uploaded_file is not None:
        df = load_data(uploaded_file)
        use_default = False
    elif use_default:
        # Try to load default file
        try:
            df = load_data('linkedin_remoteok_jobs.csv')
            if df is None:
                st.info("📁 No default data file found. Please upload a CSV file to get started.")
        except:
            st.info("📁 No default data file found. Please upload a CSV file to get started.")
    
    if df is not None and not df.empty:
        # Sidebar filters
        st.sidebar.subheader("🔍 Filters")
        
        # Source filter
        sources = ['All'] + list(df['source'].unique())
        selected_source = st.sidebar.selectbox("Select Source", sources)
        
        # Date range filter
        min_date = df['date_posted'].min().date()
        max_date = df['date_posted'].max().date()
        
        date_range = st.sidebar.date_input(
            "Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date
        )
        
        # Apply filters
        filtered_df = df.copy()
        
        if selected_source != 'All':
            filtered_df = filtered_df[filtered_df['source'] == selected_source]
        
        if len(date_range) == 2:
            start_date, end_date = date_range
            filtered_df = filtered_df[
                (filtered_df['date_posted'].dt.date >= start_date) & 
                (filtered_df['date_posted'].dt.date <= end_date)
            ]
        
        # Main dashboard
        if not filtered_df.empty:
            # Key metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Jobs", len(filtered_df))
            
            with col2:
                unique_companies = filtered_df['company'].nunique()
                st.metric("Unique Companies", unique_companies)
            
            with col3:
                unique_cities = filtered_df['city'].nunique()
                st.metric("Cities", unique_cities)
            
            with col4:
                date_range_days = (filtered_df['date_posted'].max() - filtered_df['date_posted'].min()).days
                st.metric("Date Range (Days)", date_range_days)
            
            # Row 1: Top Job Titles and Skills
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("🏆 Top 5 Most In-Demand Job Titles")
                top_titles = get_top_job_titles(filtered_df, 5)
                
                if not top_titles.empty:
                    fig = px.bar(
                        x=top_titles.values,
                        y=top_titles.index,
                        orientation='h',
                        title="Job Titles by Frequency",
                        labels={'x': 'Number of Postings', 'y': 'Job Title'},
                        color=top_titles.values,
                        color_continuous_scale='Blues'
                    )
                    fig.update_layout(height=400, showlegend=False)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No job title data available")
            
            with col2:
                st.subheader("💻 Most Frequent Skills Required")
                top_skills = get_top_skills(filtered_df, 10)
                
                if top_skills:
                    skills_df = pd.DataFrame(list(top_skills.items()), columns=['Skill', 'Count'])
                    
                    fig = px.bar(
                        skills_df,
                        x='Count',
                        y='Skill',
                        orientation='h',
                        title="Skills by Frequency",
                        labels={'Count': 'Number of Mentions', 'Skill': 'Skill'},
                        color='Count',
                        color_continuous_scale='Viridis'
                    )
                    fig.update_layout(height=400, showlegend=False)
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Skills word frequency
                    st.subheader("📋 Top Skills List")
                    skills_text = ""
                    for skill, count in list(top_skills.items())[:5]:
                        skills_text += f"**{skill}**: {count} mentions  \n"
                    st.markdown(skills_text)
                else:
                    st.info("No skills data available")
            
            # Row 2: Cities and Posting Trends
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("🏙️ Cities with Highest Number of Openings")
                top_cities = get_top_cities(filtered_df, 10)
                
                if not top_cities.empty:
                    fig = px.pie(
                        values=top_cities.values,
                        names=top_cities.index,
                        title="Job Distribution by City"
                    )
                    fig.update_traces(textposition='inside', textinfo='percent+label')
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Top cities table
                    st.subheader("📊 Top Cities Table")
                    cities_df = pd.DataFrame({
                        'City': top_cities.index,
                        'Job Count': top_cities.values
                    })
                    st.dataframe(cities_df, use_container_width=True)
                else:
                    st.info("No city data available")
            
            with col2:
                st.subheader("📈 Job Posting Trends Over Time")
                trends_data = create_posting_trends(filtered_df)
                
                if not trends_data.empty:
                    fig = px.line(
                        trends_data,
                        x='date',
                        y='job_count',
                        title="Daily Job Postings",
                        labels={'date': 'Date', 'job_count': 'Number of Job Postings'}
                    )
                    fig.update_traces(line_color='#1f77b4', line_width=3)
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Trends summary
                    st.subheader("📊 Trends Summary")
                    avg_daily = trends_data['job_count'].mean()
                    max_daily = trends_data['job_count'].max()
                    total_jobs = trends_data['job_count'].sum()
                    
                    st.markdown(f"""
                    - **Average daily postings**: {avg_daily:.1f}
                    - **Peak daily postings**: {max_daily}
                    - **Total jobs in period**: {total_jobs}
                    """)
                else:
                    st.info("No trends data available")
            
            # Additional insights
            st.subheader("🔍 Additional Insights")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.subheader("📊 Jobs by Source")
                source_counts = filtered_df['source'].value_counts()
                fig = px.bar(
                    x=source_counts.index,
                    y=source_counts.values,
                    title="Job Distribution by Source",
                    labels={'x': 'Source', 'y': 'Number of Jobs'}
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.subheader("📅 Jobs by Date Posted")
                daily_jobs = filtered_df['date_posted'].dt.date.value_counts().sort_index()
                fig = px.bar(
                    x=daily_jobs.index,
                    y=daily_jobs.values,
                    title="Jobs Posted by Date",
                    labels={'x': 'Date', 'y': 'Number of Jobs'}
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col3:
                st.subheader("🏢 Top Companies")
                top_companies = filtered_df['company'].value_counts().head(5)
                company_text = ""
                for company, count in top_companies.items():
                    company_text += f"**{company}**: {count} jobs  \n"
                st.markdown(company_text)
            
            # Raw data view
            with st.expander("📋 View Raw Data"):
                st.dataframe(filtered_df, use_container_width=True)
                
                # Download button
                csv = filtered_df.to_csv(index=False)
                st.download_button(
                    label="Download filtered data as CSV",
                    data=csv,
                    file_name=f"filtered_jobs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
        
        else:
            st.warning("No data available for the selected filters.")
    
    else:
        st.info("""
        👋 Welcome to the Job Market Dashboard!
        
        To get started:
        1. Upload your job data CSV file using the sidebar, or
        2. Place your CSV file named 'linkedin_remoteok_jobs.csv' in the same directory
        3. Check the 'Use sample data' option to load the default file
        
        The CSV should contain columns: title, company, location, date_posted, skills, source
        """)

if __name__ == "__main__":
    main()