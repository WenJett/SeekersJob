# is3107_DataEngineering
Repository for IS3107 project: **JobPulse**

---
# Set Up

## UI with Streamlit
1. Set working directory to `/Web_App`
2. `streamlit run app.py`
3. Press `Ctrl + C` to terminate streamlit

## Airflow 
1. Navigate to bash 
2. Create virtual environment using `python3 -m venv .venv \ source .venv/bin/activate`
3. Set AIRFLOW_HOME `export $AIRFLOW_HOME={pwd}`
4. Run Airflow `airflow standalone`
5. Navigate to [http://localhost:8070/](http://localhost:8070/) for airflow web server
6. Press `Ctrl + C` to terminate airflow

---
# Files

## Config files
- Standalone_admin_password.txt - password for local airflow 
- user_credentials.txt - credentials for LinkedIn account used for web scraping 
- credentials.py - stores the credentials for the PostGresSQL Database

## Archive (outdated)
- Back up store of older project artifacts

## Indeed_WebScraping
- Scraping code for Indeed website
- Raw data from scraping

## InternSg_WebScraping
- Scraping code for InternSG website
- Raw data from scraping

## JobStreet_WebScraping
- Scraping code for JobStreet website
- Raw data from scraping

## LinkedIn_WebScraping
- Scraping code for LinkedIn website
- Raw data from scraping

## MyCareersFuture_WebScraping
- Scraping code for MyCareersFuture website
- Raw data from scraping

## Job Analysis
- Data / reference code for Web_App applications
- Jobs_analysis_with_Spacy.ipynb contains additional exploration of using part-of-speech tagging to identify and match jobs with “skills” tags. Word cloud from the dashboard is derived from here.
- recc_system.ipynb contains additional exploration of recommendation system ML models. Job recommender is derived from here.

## Web_App
- app.py is the main file for the web application that shows the downstream application

## dags
- DAG.py is the DAG file for Apache airflow

## remaining CSV files
- modified and cleaned versions of the raw data files

## consolidate.csv
- final CSV of all combined data sources after pre-processing and cleaning
