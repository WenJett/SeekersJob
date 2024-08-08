import os
import csv
import time
import pandas as pd
import numpy as np
import re

from csv import writer
from time import sleep
from random import randint

from datetime import datetime, timedelta
# from selenium import webdriver
# from selenium.webdriver.common.by import By
from airflow.providers.postgres.hooks.postgres import PostgresHook
import psycopg2
from airflow.models import TaskInstance
from airflow.utils.state import State
from airflow.decorators import dag, task
from datetime import datetime

def load_csv(consolidated_file_path):
    try:
        df = pd.read_csv(consolidated_file_path)
        df = df.fillna("Null")
        return df
    except:
        return pd.DataFrame({'job_title':['test1'], 'description':['test1'], 'company':['test1'], 'salary_range':['test1']})

@dag(dag_id='3107_project_past2', schedule_interval=None, catchup=False, tags=['project'])
def extract_transform_load():

    @task
    def extract_indeed_jobs():
        indeed_jobs_url = r"Indeed_WebScraping/indeed_jobs_raw.csv"
        return indeed_jobs_url

    @task
    def clean_indeed_jobs(indeed_jobs_url):
        data = pd.read_csv(indeed_jobs_url)

        data.dropna(subset=['Job Title'], inplace=True)

        # Remove additional text of -job post in Job Title Col
        for index, row in data.iterrows():
            parts = row['Job Title'].split('-', 1)
            data.loc[index, 'Job Title'] = parts [0]

        # To seperate the salary and Job Type during web scraping
        for index, row in data.iterrows():
            if row['Salary'] == "nan":
                pass
            elif '$' not in str(row['Salary']):
                data.loc[index, 'Job Type'] = row['Salary']
                data.loc[index, 'Salary'] = None
            elif "Up to" in row['Salary'] and row['Salary'].count('-') >= 1:
                parts = row['Salary'].split('-', 1)
                data.loc[index, 'Salary'] = parts[0] 
                data.loc[index, 'Job Type'] = parts[1]
            elif row['Salary'].count('-') >= 2:
                parts = row['Salary'].split('-', 2)
                data.loc[index, 'Salary'] = parts[0] + '-' + parts[1]
                data.loc[index, 'Job Type'] = parts[2]

        # To indicate the listing date from posted date information
        # Convert text to datetime for manipulation 
        data['Today Date'] = pd.to_datetime(data['Today Date'], format='%d/%m/%y')

        # Convert 'Today Date' to string with 'day/month/year' format
        data['Today Date'] = data['Today Date'].dt.strftime('%d/%m/%Y')

        for index, row in data.iterrows():
            if not re.search(r'\d', row['Posted Date Raw']):
                data.loc[index, 'Job Posting Date'] = row['Today Date']
            else:
                days = re.findall(r'\d+', row['Posted Date Raw'])
                days_int = sum(map(int, days))
                listing_date = pd.to_datetime(row['Today Date'], format='%d/%m/%Y') - pd.DateOffset(days=days_int)
                data.loc[index, 'Job Posting Date'] = listing_date.strftime('%d/%m/%Y')  # Change the format here

        # Convert 'Job Posting Date' column to datetime dtype
        data['Job Posting Date'] = pd.to_datetime(data['Job Posting Date'], format='%d/%m/%Y')


        new_file_path = 'indeed_jobs_modified.csv'
        data.to_csv(new_file_path, index=False)

        return new_file_path

    @task
    def extract_internsg_jobs():
        file_path = "InternSg_WebScraping/internSG_jobs.csv"
        return file_path
         
    @task
    def extract_jobstreet_jobs():
        file_path = "JobStreet_WebScraping/jobstreet_jobs_data.csv"
        return file_path
    
    @task
    def extract_mycareerfuture_jobs():
        file_path = "MyCareersFuture_WebScraping/jobs_ai.csv"
        return file_path
    
    @task
    def clean_mycareerfuture_jobs(mycareerfuture_url):
        data = pd.read_csv(mycareerfuture_url)

        data['Salary'] = data['salary_lower'].astype(str) + "-" + data['salary_upper'].astype(str) + " " + data['salary_period']
        new_file_path = 'mycareersfuture_jobs_modified.csv'
        data.to_csv(new_file_path, index = False)

        return new_file_path

    @task
    def extract_linkedin_jobs():
        file_path = "LinkedIn_WebScraping/linkedin_jobs.csv"
        return file_path
    
    @task
    def rename_and_filter_columns(df_file_path):

        mapping = {
            "job title": ["Designation", "Job Title", "titles", "title", "job_title", 'job-title'],
            "description": ["Job Description", "teasers", "description", "job_description", 'Job_txt'],
            "company": ["Company", "companies", "company", "company_name"],
            "salary range": ["Allowance / Remuneration", "Salary", "salaries"],
            "url": ["URL", "link", 'url']
        }

        # Extract the file name
        file_name = os.path.basename(df_file_path)

        # Split the file name and extension
        name, extension = os.path.splitext(file_name)

        df = pd.read_csv(df_file_path)

        # Add 'new' before the file name
        new_file_name = 'new_' + name + extension

        directory_path = os.path.dirname(df_file_path)

        new_file_path = os.path.join(directory_path, new_file_name)

        renamed_columns = {}
        for new_name, old_names in mapping.items():
            for old_name in old_names:
                if old_name in df.columns:
                    renamed_columns[old_name] = new_name
                    break  # Stop looking once we find the first match
            if new_name not in renamed_columns.values():  # Check if the new_name is not yet in the renamed_columns
                df[new_name] = np.nan  # If a column is missing, create it with all null values
        # Rename the columns and re-order based on the mapping dictionary keys to ensure consistency
        new_df =  df.rename(columns=renamed_columns)[list(mapping.keys())]
        new_df.to_csv(new_file_path, index = False)

        return new_file_path
    
    @task
    def consolidate_files(df1, df2, df3, df4, df5):
        df1_file = pd.read_csv(df1)
        df2_file = pd.read_csv(df2)
        df3_file = pd.read_csv(df3)
        df4_file = pd.read_csv(df4)
        df5_file = pd.read_csv(df5)

        consolidated_df = pd.concat([df1_file,df2_file,df3_file,df4_file, df5_file])

        directory_path = os.path.dirname(df1)
        new_file_path = os.path.join(directory_path, 'consolidated.csv')
        consolidated_df = consolidated_df[consolidated_df['description'].notnull()]
        consolidated_df.to_csv(new_file_path, index = False)

        return new_file_path
    
    # add load database
    @task 
    def upload_data(file_path):
        postgres_hook = PostgresHook(postgres_conn_id="postgres_azurehost")
        df = load_csv(file_path)
        print('{} rows loaded ...'.format(len(df)))
        truncate_sql = """TRUNCATE TABLE consolidatedJobs;"""
        postgres_hook.run(truncate_sql, autocommit=True)
        print('previous data truncated ...')
        load_sql = """
        INSERT INTO consolidatedJobs (job_title, description, company, salary_range, url)
        VALUES (%(job_title)s, %(description)s, %(company)s, %(salary_range)s, %(url)s);
        """
        for idx, row in df.head().iterrows():
            row_entry = {
            'job_title': row['job title'],
            'description': row['description'],
            'company': row['company'],
            'salary_range': row['salary range'],
            'url': row['url']
            }
            # Use the Airflow PostgresHook to execute the SQL
            postgres_hook.run(load_sql, parameters=row_entry, autocommit=True)
        print('{} rows of data uploaded ...'.format(len(df)))

    indeed_jobs = extract_indeed_jobs()
    indeed_jobs = clean_indeed_jobs(indeed_jobs)

    internsg_jobs = extract_internsg_jobs()

    jobstreet_jobs = extract_jobstreet_jobs()

    mycareerfuture_jobs =  extract_mycareerfuture_jobs()
    mycareerfuture_jobs = clean_mycareerfuture_jobs(mycareerfuture_jobs)

    linkedin_jobs = extract_linkedin_jobs()


    new_indeed_jobs = rename_and_filter_columns(indeed_jobs)
    new_internsg_jobs =  rename_and_filter_columns(internsg_jobs)
    new_jobstreet_jobs = rename_and_filter_columns(jobstreet_jobs)
    new_mycareerfuture = rename_and_filter_columns(mycareerfuture_jobs)
    new_linkedin = rename_and_filter_columns(linkedin_jobs)


    consolidated_jobs = consolidate_files(new_indeed_jobs, new_internsg_jobs, new_jobstreet_jobs, new_mycareerfuture, new_linkedin)

    upload_data(consolidated_jobs)

dag = extract_transform_load()
