import streamlit as st
import time
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt 
import altair as alt
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

import numpy as np
import json
import spacy
from spacy.pipeline import EntityRuler
from spacy.cli import download
from wordcloud import WordCloud

st.set_page_config(layout="wide")

# Set the title of the web app
st.title('Singapore Job Market Insights')

# Add a brief description of the project
st.write('Welcome to the Singapore Job Market Insights Web Application! Get personalised job recommendations and explore key trends, skill demand, and salary ranges in the Singapore job market.')

st.divider()

def load_data():
        select_table_query = """SELECT * FROM consolidatedJobs"""
        conn = psycopg2.connect(database="is3107JobsDB", user='is3107Postgres', password='JobsDBProject3107', host='is3107.postgres.database.azure.com', port= '5432')
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(select_table_query)
        result = cursor.fetchall()
        # print(result)
        conn.commit()
        conn.close()        
        colnames = ['job_title', 'description', 'company', 'salary_range', 'url']
        # pd.DataFrame(result, columns=colnames).to_csv(load_file_path)
        return pd.DataFrame(result, columns=colnames)

def preprocess_job_title(title):
    title = title.strip()  
    return title

def map_company_name(name):
    company_mapping = {
        '4e Exchange': '4e',
        'A*STAR Research Entities (A*STAR)': 'A*STAR - Agency for Science, Technology and Research',
        'A*STAR Agency for Science, Technology and Research': 'A*STAR - Agency for Science, Technology and Research',
        'Agency for Science, Technology and Research (A*STAR)': 'A*STAR - Agency for Science, Technology and Research',
        'Activate Interactive Pte Ltd': 'Activate Interactive',
        'ADECCO PERSONNEL PTE LTD': 'Adecco',
        'Adecco Personnel Pte Ltd.': 'Adecco',
        'Adecco Personnel': 'Adecco',
        'Advanced Micro Devices, Inc': 'AMD',
        'Advanced Micro Devices (S) Pte Ltd': 'AMD',
        'Agoda Company Pte. Ltd': 'Agoda',
        'AIA SINGAPORE PRIVATE LIMITED - GOH HOCK CHYE SAMUEL': 'AIA',
        'AIA Singapore': 'AIA',
        'AMBITION GROUP SINGAPORE PTE. LTD.': 'Ambition',
        'AIRWALLEX (SINGAPORE) PTE. LTD.': 'Airwallex',
        'Apple Inc.': 'Apple',
        'ARGYLL SCOTT CONSULTING PTE. LTD.': 'Argyll Scott',
        'Ascenda Loyalty': 'Ascenda',
        'Beyondsoft International (Singapore) Pte Ltd': 'Beyondsoft International',
        'Beyondsoft Singapore': 'Beyondsoft International',
        'BYTEDANCE PTE. LTD.': 'ByteDance',
        'ComfortDelGro Transportation': 'Comfort Transportation Pte Ltd',
        'Changi Airport Group (Singapore) Pte. Ltd.': 'Changi Airport Group',
        'Chevron Singapore Pte Ltd': 'Chevron',
        'Carousell Pte Ltd': 'Carousell Group',
        'DBS Bank Limited': 'DBS',
        'DBS BANK LTD.': 'DBS',
        'Crypto': 'Crypto.com',
        'Disney Media & Entertainment Distribution': 'Disney',
        'CTES CONSULTING PTE. LTD.': 'CTES Consulting Pte Ltd',
        'DATATURE PTE. LTD.': 'Datature',
        'Danone Specialized Nutrition (Singapore) Private Limited': 'Danone',
        'DURAPOWER TECHNOLOGY (SINGAPORE) PTE. LTD.': 'Durapower Technology (Singapore) Pte Ltd',
        'Dyson Operations Pte Ltd': 'Dyson',
        'DYSON OPERATIONS PTE. LTD.': 'Dyson',
        'ECCO Asia Pacific (Singapore) Pte Ltd': 'ECCO',
        'ECCO Asia Pacific (Singapore) Pte Ltd': 'ECCO',
        'Elitez India': 'Elitez Pte Ltd',
        'ELLIOTT MOSS CONSULTING PTE. LTD.': 'Elliott Moss Consulting',
        'ENCORA TECHNOLOGIES PTE. LTD.': 'Encora',
        'FCM Travel Asia': 'FCM',
        'FEATURESPACE PTE. LTD.': 'Featurespace',
        'FLINTEX CONSULTING PTE. LTD.': 'Flintex Consulting Pte Ltd',
        'FOODPANDA SINGAPORE PTE. LTD.': 'foodpanda',
        'Genie Financial Services': 'Genie',
        'Genie Financial Services Pte Ltd': 'Genie',
        'Geotab Inc.': 'Geotab',
        'GIC Investment': 'GIC',
        'GIC Private Limited': 'GIC',
        'Google Inc.': 'Google',
        'GRABTAXI HOLDINGS PTE. LTD.': 'Grab',
        'Gunvor Group Ltd': 'Gunvor Singapore Pte Ltd',
        'GRAVITON RESEARCH CAPITAL (SINGAPORE) PTE. LTD.': 'Graviton Research Capital LLP',
        'GMP Group HQ': 'GMP Group',
        'GMP TECHNOLOGIES (S) PTE LTD': 'GMP Group',
        'Hays Specialist Recruitment Pte Ltd': 'Hays',
        'HYPERSCAL SOLUTIONS PTE. LTD.': 'Hyperscal Solutions',
        'Hogarth Worldwide': 'Hogarth',
        'Hudson Global Resources ( HQ )': 'Hudson',
        'Hewlett Packard Enterprise': 'Hewlett Packard',
        'HYPERGAI PTE. LTD.': 'HyperGAI',
        'HYUNDAI MOTOR GROUP INNOVATION CENTER IN SINGAPORE PTE. LTD.': 'Hyundai Motor Group Innovation Center Singapore (HMGICS)',
        'Holmusk (KKT Technology Pte Ltd)': 'Holmusk',
        'IBM SERVICES TALENT DELIVERY PTE. LTD.': 'IBM',
        'IBM SINGAPORE PTE LTD': 'IBM',
        'Institute Of Data Pte. Ltd.': 'Institute of Data',
        'ITCAN PTE. LIMITED': 'ITCAN Pte Ltd',
        'IFS Capital Group': 'IFS Capital Limited',
        'Infosight consulting': 'Infosight Consuting',
        'Infosys': 'Infosys Consulting',
        'JLL Technologies': 'JLL',
        'JONDAVIDSON PTE. LTD.': 'JonDavidson Pte Ltd',
        'JPMorgan Chase & Co.': 'JPMorgan Chase & Co',
        'Kerry Consulting Pte Ltd': 'Kerry Consulting',
        'KEYRUS SINGAPORE PTE. LTD.': 'Keyrus',
        'LITE-ON SINGAPORE PTE. LTD.': 'LITE-ON SINGAPORE PTE LTD',
        'LTIMINDTREE LIMITED SINGAPORE BRANCH': 'LTIMindtree',
        'Luxoft Singapore': 'Luxoft',
        'Manpower Staffing Services (S) Pte Ltd - Head Office': 'Manpower Singapore',
        'Manpower': 'Manpower Singapore',
        'MANPOWER STAFFING SERVICES (SINGAPORE) PTE LTD': 'Manpower Singapore',
        'Mediacorp Pte. Ltd.': 'Mediacorp Pte Ltd',
        'Mediatek Singapore Pte Ltd': 'MediaTek',
        'MedMosaic Pte. Ltd.': 'MedMosaic',
        'MICRON SEMICONDUCTOR ASIA OPERATIONS PTE LTD': 'Micron',
        'Micron Technology': 'Micron',
        'MORGAN MCKINLEY PTE. LTD.': 'Morgan McKinley',
        'NANYANG TECHNOLOGICAL UNIVERSITY': 'Nanyang Technological University',
        'Nanyang Technological University Singapore': 'Nanyang Technological University',
        'NTU (Nanyang Technology University- Main Office-HR)': 'Nanyang Technological University',
        'NATIONAL UNIVERSITY HEALTH SYSTEM PTE. LTD.': 'National University Health System',
        'NCS Pte Ltd': 'NCS',
        'NCS Group': 'NCS',
        'NEC Australia': 'NEC Corporation',
        'NEW TONE CONSULTING PTE. LTD.': 'Newtone consulting',
        'Nicoll Curtin Group': 'Nicoll Curtin',
        'NTT DATA SINGAPORE PTE. LTD.': 'NTT SINGAPORE PTE. LTD.',
        'OCBC Bank': 'OCBC',
        'OCBC (Singapore)': 'OCBC',
        'OmniVision Technologies Singapore Pte Ltd': 'OMNIVISION',
        'OPTIMUM SOLUTIONS (SINGAPORE) PTE LTD': 'Optimum Solutions Pte Ltd',
        'People Profilers Pte Ltd': 'People Profilers',
        'Peoplebank Singapore Pte Ltd': 'Peoplebank',
        'PERCEPT SOLUTIONS PTE. LTD.': 'Percept Solutions',
        'PERSOLKELLY SINGAPORE PTE. LTD.': 'PERSOLKELLY',
        'PERSOLKELLY Singapore': 'PERSOLKELLY',
        'PERSOLKELLY Singapore Pte Ltd (Formerly Kelly Services Singapore Pte Ltd)': 'PERSOLKELLY',
        'Prudential (WM Group- Linda Quah)': 'Prudential',
        'Prudential Assurance Company Singapore': 'Prudential',
        'PRUDENTIAL ASSURANCE COMPANY SINGAPORE (PTE) LIMITED': 'Prudential',
        'Prudential Hong Kong': 'Prudential',
        'Prudential plc': 'Prudential',
        'PSA International': 'PSA Corporation Limited',
        'PSA Singapore': 'PSA Corporation Limited',
        'PYXIS CF PTE. LTD.': 'Pyxis CF Pte Ltd',
        'QCP Capital': 'QCP',
        'QUESSCORP SINGAPORE PTE. LTD.': 'Quess Corp Limited',
        'Rakuten Insight Singapore Pte., Ltd.': 'Rakuten Asia Pte Ltd',
        'RANDSTAD PTE. LIMITED': 'Randstad',
        'Randstad Singapore': 'Randstad',
        'Renesas Electronics Singapore Pte. Ltd.': 'Renesas Electronics',
        'RGF Professional Recruitment': 'RGF',
        'RGF Talent Solutions Singapore Pte Ltd': 'RGF',
        'Robert Half International Pte Ltd': 'Robert Half',
        'RECRUIT EXPRESS PTE LTD': 'Recruit Express Group',
        'SAFRAN ELECTRONICS & DEFENSE': 'Safran',
        'SAKSOFT PTE LIMITED': 'Saksoft Pte Ltd',
        'Sanderson-iKas Singapore Pte Ltd': 'Sanderson-iKas Singapore ',
        'SAP Asia Pte Ltd': 'SAP',
        'SAP ASIA PTE. LTD.': 'SAP',
        'SATS Ltd.': 'SATS',
        'Schroder Investment Management (Singapore) Ltd': 'Schroders',
        'SCIENTEC CONSULTING PTE. LTD.': 'ScienTec Consulting',
        'ScienTec Consulting Pte Ltd': 'ScienTec Consulting',
        'Sea Limited': 'Sea',
        'SHELL INFOTECH PTE. LTD.': 'Shell',
        'SHOPEE SINGAPORE PRIVATE LIMITED': 'Shopee',
        'SingHealth Group': 'SingHealth',
        'Smith+Nephew': 'Smith & Nephew',
        'SoftwareONE Pte Ltd': 'SoftwareOne',
        'ST Engineering Ltd': 'ST Engineering',
        'StarHub Ltd': 'StarHub',
        'SYNAPXE PTE. LTD.': 'Synapxe',
        'Talent Trader Group Pte Ltd': 'Talent Trader Group',
        'TE Connectivity Singapore': 'TE Connectivity',
        'TEKsystems (Allegis Group Singapore Pte Ltd)': 'TEKsystems',
        'Temus Pte. Ltd.': 'Temus',
        'THAKRAL ONE PTE. LTD.': 'THAKRAL ONE PTE LTD',
        'TECHNOPALS CONSULTANTS PTE. LTD.': 'TECHNOPALS PTE. LTD.',
        'Thales Singapore Pte. Ltd.': 'Thales', 
        'TIKTOK PTE. LTD.': 'TikTok',
        'THE SOFTWARE PRACTICE PTE. LTD.': 'The Software Practice',
        'Univers Pte Ltd': 'Univers',
        'Unison Consulting Pte. Ltd.': 'Unison Consulting',
        'UNITED OVERSEAS BANK LIMITED': 'UOB',
        'United Overseas Bank Limited (UOB)': 'UOB',
        'United Overseas Bank Ltd': 'UOB',
        'Upskills Consultancy Services': 'Upskills',
        'WPH DIGITAL PTE. LTD.': 'WPH',
        'Zoom Video Communications, Inc.': 'Zoom'
    }

    return company_mapping.get(name, name)

# # Generate skill patterns
# file_txt = open("Skills_in_Demand.txt", 'r', encoding='cp1252')
# list_skills_in_demand = []
# for x in file_txt.readlines():
#     list_skills_in_demand.append(x.strip())
# file_txt.close()

# rule_patterns = []
# for skill in list_skills_in_demand:
#     pattern = []
#     for elt in skill.split(" "):
#         pattern.append({"LOWER": elt})
        
#     json_data = {"label": "SKILL", "pattern": pattern}
#     json_string = json.dumps(json_data, ensure_ascii=True)
#     rule_patterns.append(json_string)

# file_jsonl = open("Skill_patterns.jsonl", "w")
# for pattern in rule_patterns:
#     file_jsonl.write(pattern + "\n")
# file_jsonl.close()

# print("Skill patterns JSONL file created!")

# Load skill patterns
skill_pattern_path = "Skill_patterns.jsonl"
nlp = spacy.load("en_core_web_lg")

ruler = nlp.add_pipe("entity_ruler")
ruler.from_disk(skill_pattern_path)

def extract_skills(description):
    doc = nlp(description)
    skills = []
    for ent in doc.ents:
        if ent.label_ == "SKILL":
            skills.append(ent.text.lower())
    return skills

def generate_word_cloud(df):
    # Filter out non-string values from skills_list
    skills_list = [skill for skill in df['skills'].explode().tolist() if isinstance(skill, str)]
    
    # Join the filtered skills_list into a single string
    list_skills = " ".join(skills_list)

    wordcloud = WordCloud(background_color='black', max_words=100, width=800, height=400, max_font_size=250, collocations=False)
    wordcloud.generate(list_skills)

    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud.recolor(colormap="Blues"), interpolation='bilinear')
    plt.axis("off")
    plt.title(f"Skills in demand\n", size=20, weight='bold')

    # Pass the figure explicitly to st.pyplot()
    st.pyplot(plt)

jobs_data = load_data()

# Text preprocessing
tfidf_vectorizer = TfidfVectorizer(stop_words='english')
tfidf_matrix = tfidf_vectorizer.fit_transform(jobs_data['description'])

# Function to recommend jobs based on input text
def recommend_jobs(input_text, df, tfidf_matrix, tfidf_vectorizer, top_n=5):
    input_vector = tfidf_vectorizer.transform([input_text])
    cosine_similarities = cosine_similarity(input_vector, tfidf_matrix).flatten()
    related_jobs_indices = cosine_similarities.argsort()[:-top_n-1:-1]
    recommended_jobs = df.iloc[related_jobs_indices]
    return recommended_jobs

# Function for displaying job recommendation page
def display_job_recommendation_page():
    st.title('Job Recommendation   :computer:')
    st.write('Input your resume details and receive personalised job recommendations.')

    # For user to input resume details
    resume_details = st.text_area("Resume Details", key='resume_details_input')

    if st.button("Submit"):
        # Display status indicator while retrieving job recommendations
        with st.spinner("Retrieving job recommendations..."):
            # Run ML model to process user input and return job results
            recommended_jobs = recommend_jobs(resume_details, jobs_data, tfidf_matrix, tfidf_vectorizer)

            st.markdown("---")

            # Display job recommendations to the user
            if recommended_jobs.empty:
                st.write("No job recommendations found.")
            else:
                st.write("Job recommendations retrieved successfully!")
                st.write(recommended_jobs) 

# Function for displaying dashboard page
def display_dashboard_page():
    st.title('Dashboard   :bar_chart:')
    st.write('Visualisations and insights about the Singapore job market.')

    #Load data from the database
    job_data = load_data()

    # Preprocess and map company names
    job_data['company'] = job_data['company'].apply(map_company_name)

    st.header('Top 10 Companies Hiring')
    st.subheader('Explore the leading employers in Singapore')

    # Top 10 Companies
    top_10_companies = job_data['company'].value_counts().head(10).sort_values(ascending=True)
    top_10_companies_df = pd.DataFrame({'Company': top_10_companies.index, 'Number of Job Postings': top_10_companies.values})

    chart = alt.Chart(top_10_companies_df).mark_bar().encode(
        y=alt.Y('Company:N', sort='-x'),
        x='Number of Job Postings:Q'
    ).properties(
        height=400
    )
    st.altair_chart(chart, use_container_width=True)

    # Preprocess job titles
    job_data['job_title'] = job_data['job_title'].apply(preprocess_job_title)

    st.divider()

    st.header('In-demand Jobs')
    st.subheader('Find out the most in-demand jobs in Singapore')

    # Most Common 15 Job Titles
    job_title_counts = job_data['job_title'].value_counts().head(10).reset_index()
    job_title_counts.columns = ['Job Title', 'Count']

    chart = alt.Chart(job_title_counts).mark_bar().encode(
        y=alt.Y('Job Title:N', sort='-x'),
        x='Count:Q'
    ).properties(
        height=400
    )
    st.altair_chart(chart, use_container_width=True)

    st.divider()

    st.header('Salary Ranges')
    st.subheader('Explore salary ranges across different industries')

    salary_ranges = job_data['salary_range'].value_counts().head(10).reset_index()
    salary_ranges.columns = ['Salary Range', 'Count']
    salary_ranges = salary_ranges[salary_ranges['Salary Range'] != 'Null']

    chart = alt.Chart(salary_ranges).mark_bar().encode(
        y=alt.Y('Salary Range:N', sort='-x'),
        x='Count:Q'
    ).properties(
        height=400
    )
    st.altair_chart(chart, use_container_width=True)

    st.divider()

    st.header('Customised Company View')
    st.subheader('View specific company job listings')

    job_data['company'] = job_data['company'].apply(map_company_name)

    # Create dropdown for selecting company
    selected_company = st.selectbox('Select Company', ['All'] + list(job_data['company'].unique()))

    # Placeholder for job title dropdown
    job_title_placeholder = st.empty()

    if selected_company != 'All':
        # Filter job data based on selected company
        filtered_job_data = job_data[job_data['company'] == selected_company]

        # Get job titles corresponding to the selected company
        available_job_titles = filtered_job_data['job_title'].unique()

        # Create dropdown for selecting job title
        selected_job_title = job_title_placeholder.selectbox('Select Job Title', ['All'] + list(available_job_titles))

        # Filter data based on selected job title
        if selected_job_title != 'All':
            filtered_data = filtered_job_data[filtered_job_data['job_title'] == selected_job_title]
        else:
            filtered_data = filtered_job_data

        # Display filtered data
        st.write(filtered_data)

    else:
        st.write("Please select a company to see available job titles.")

    st.divider()

    st.header('Salary Range Comparison')
    st.subheader('Explore and compare salary information across different companies')

    # Preprocess and map company names
    job_data['company'] = job_data['company'].apply(map_company_name)

    # Group data by job title
    grouped_data = job_data.groupby('job_title')

    # Find groups with more than one company and non-null salary range
    duplicate_groups = grouped_data.filter(lambda x: len(x) > 1 and (x['salary_range'] != 'Null').any() and pd.notnull(x['salary_range']).any())

    if not duplicate_groups.empty:

        # Get unique job titles from the duplicate groups
        unique_job_titles = duplicate_groups['job_title'].unique()

        # Create dropdown for selecting job title
        selected_job_title = st.selectbox('Select Job Title', unique_job_titles)

        if selected_job_title:
            # Filter data based on selected job title
            filtered_data = duplicate_groups[duplicate_groups['job_title'] == selected_job_title]
            
            # Drop duplicate entries for the same company
            filtered_data = filtered_data.drop_duplicates(subset=['company'])
            
            # Display salary range information for different companies
            st.write(filtered_data[['company', 'salary_range']])
        else:
            st.write('Please select a job title.')
    else:
        st.success("No comparison cases available.")

    st.divider()

    st.header('Skills Overview')
    st.subheader('Word Cloud displaying skills demand')

    # For wordcloud
    job_data["skills"] = job_data["description"].apply(extract_skills)
    generate_word_cloud(job_data) 

# Create navigation sidebar
st.sidebar.title('Singapore Job Market Insights')
page = st.sidebar.radio('Navigation', ['Job Recommendation', 'Dashboard'])

st.sidebar.write(""" 
                 ## About
                 The Singapore Job Market Insights is an interface designed to enable users to explore the job landscape in Singapore comprehensively, gaining insights into job trends, skill demand, salary ranges, and popular job titles across industries. 
                   
                 This serves as a valuable resource for job seekers, hiring managers, and career planners, providing them with real-time market dynamics to make informed decisions.
                 """)

# Display selected page
if page == 'Job Recommendation':
    display_job_recommendation_page()
elif page == 'Dashboard':
    display_dashboard_page()