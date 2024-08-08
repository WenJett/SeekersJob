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
from selenium import webdriver
from selenium.webdriver.common.by import By
from airflow.models import TaskInstance
from airflow.utils.state import State
from airflow.decorators import dag, task
from datetime import datetime

@dag(dag_id='3107_project_past', schedule_interval=None, catchup=False, tags=['project'])
def extract_transform_load():

    @task
    def extract_indeed_jobs(**context):
        job_ = ['business+analyst' ,'data+analyst','software+engineer']
        loc = 'Singapore'

        paginaton_url_ = 'https://sg.indeed.com/jobs?q={}&l={}&sort=date&start={}'

        driver = webdriver.Chrome()

        p_ = []
        salary_list_ = []
        company_list_ = []
        location_list_ = []
        job_description_list_ = []
        posted_date_list_ = []
        url_list_ = []

        # Loop through each job and location combination
        for job in job_:
            for i in range(0, 1):  # Scrape 7 pages for each job for latest job postings
                driver.get(paginaton_url_.format(job, loc, i * 10))
                sleep(randint(2, 5))

                # Scroll through the page and obtain job listings one by one
                job_page = driver.find_element(By.ID, "mosaic-jobResults")
                jobs = job_page.find_elements(By.CLASS_NAME, "job_seen_beacon")

                for idx, jj in enumerate(jobs):
                            try:
                                # Scroll to the current job listing
                                driver.execute_script("arguments[0].scrollIntoView(true);", jj)
                                sleep(1)

                                # Click on the job listing
                                jj.click()
                                sleep(2)  # Add a short pause after clicking

                            except:
                                close_button = driver.find_element(By.CSS_SELECTOR, "button[aria-label='close']")
                                close_button.click()
                                sleep(1)  # Add a short pause after closing the pop-up

                            # job title
                            try:
                                job_title = driver.find_element(By.CLASS_NAME, "jobsearch-JobInfoHeader-title-container").text
                                p_.append(job_title)
                            except:
                                p_.append(None)
                                
                            # salary
                            try:
                                salary = driver.find_element(By.CSS_SELECTOR, "div[data-testid='jobsearch-OtherJobDetailsContainer']").text
                                salary_list_.append(salary)
                            except:
                                salary_list_.append(None)
                    

                            # company name
                            try:
                                company = driver.find_element(By.CSS_SELECTOR, "div[data-testid='inlineHeader-companyName']").text
                                company_list_.append(company)
                            except:
                                company_list_.append(None)    

                            # company location
                            try:
                                location = driver.find_element(By.CSS_SELECTOR, "div[data-testid='inlineHeader-companyLocation']").text
                                location_list_.append(location)
                            except:
                                location_list_.append(None)

                            # job description
                            try:
                                job_description = driver.find_element(By.ID, "jobDescriptionText").text
                                job_description_list_.append(job_description)
                            except:
                                job_description_list_.append(None)
                            
                            # posting date
                            try:
                                posted_date_element = driver.find_element(By.CSS_SELECTOR, "span[data-testid='myJobsStateDate']")
                                posted_date = posted_date_element.get_attribute("textContent")
                                posted_date_list_.append(posted_date)
                            except:
                                posted_date_list_.append(None)
                            
                            # URL
                            try:
                                job_url = driver.current_url
                                url_list_.append(job_url)
                            except:
                                url_list_.append(None)
                                
        file_name = 'indeed_jobs_raw.csv'

        # Write data to CSV file
        with open(file_name, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['Job Title', 'Company', 'Salary', 'Job Type', 'Location', 'Job Description', 'Posted Date Raw', 'Today Date', 'Job Posting Date','URL']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            today_date = datetime.now()
            today_date = today_date.strftime("%d/%m/%Y")
            
            writer.writeheader()
            for job, company, salary, location, job_description, posted_date, url in zip(p_, company_list_, salary_list_, location_list_, job_description_list_, posted_date_list_, url_list_):
                writer.writerow({'Job Title': job, 'Company': company, 'Salary': salary, 'Location': location, 'Job Description': job_description, 'Posted Date Raw': posted_date, 'Today Date': today_date, 'URL': url})  

        # Set the status of the task to success
        task_instance = context['task_instance']

        # Set the state to success
        task_instance.set_state(State.SUCCESS)

        driver.quit()

        return file_name

    @task
    def clean_indeed_jobs(indeed_jobs_url):
        indeed_jobs_url = 'indeed_jobs_raw.csv'
        data = pd.read_csv(indeed_jobs_url)

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
        data['Today Date'] = pd.to_datetime(data['Today Date'], format='%d/%m/%Y')

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
        file_path = r'/Users/jett/Desktop/internSG_jobs.csv'
        return file_path
         
    @task
    def extract_jobstreet_jobs():
        file_path = r'/Users/jett/Desktop/jobstreet_jobs_data.csv'
        return file_path
    
    @task
    def extract_mycareerfuture_jobs():
        file_path_1 = r'/Users/jett/Desktop/jobs_ai.csv'
        return file_path_1
    
    @task
    def extract_mycareerfuture_jobs_2():
        file_path_2 = r'/Users/jett/Desktop/jobs_ml.csv'
        return file_path_2
    
    @task
    def extract_mycareerfuture_jobs_3():
        file_path_3 = r'/Users/jett/Desktop/jobs_data-science.csv'
        return file_path_3
    
    @task
    def extract_mycareerfuture_jobs_4():
        file_path_4 = r'/Users/jett/Desktop/jobs_data-analytics.csv'
        return file_path_4

    @task
    def extract_linkedin_jobs():
        file_path_1 = r'/Users/jett/Desktop/machineLearningEngineer_jobs.csv'
        return file_path_1

    @task
    def extract_linkedin_jobs_2():
        file_path_2 = r'/Users/jett/Desktop/dataAnalyst_jobs.csv'
        return file_path_2
    
    @task
    def extract_linkedin_jobs_3():
        file_path_3 = r'/Users/jett/Desktop/dataScientist_jobs.csv'
        return file_path_3
    
    @task
    def rename_and_filter_columns(df_file_path):

        mapping = {
            "job title": ["Designation", "Job Title", "titles", "title"],
            "description": ["Job Description", "teasers", "description"],
            "company": ["Company", "companies", "company"],
            "salary range": ["Allowance / Remuneration", "Salary", "salaries"]
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
    def consolidate_files(df1, df2, df3, df4, df5, df6, df7, df8, df9 , df10):
        df1_file = pd.read_csv(df1)
        df2_file = pd.read_csv(df2)
        df3_file = pd.read_csv(df3)
        df4_file = pd.read_csv(df4)
        df5_file = pd.read_csv(df5)
        df6_file = pd.read_csv(df6)
        df7_file = pd.read_csv(df7)
        df8_file = pd.read_csv(df8)
        df9_file = pd.read_csv(df9)
        df10_file  = pd.read_csv(df10)

        consolidated_df = pd.concat([df1_file,df2_file,df3_file,df4_file, df5_file,df6_file,df7_file,df8_file,df9_file,df10_file])

        directory_path = os.path.dirname(df1)
        new_file_path = os.path.join(directory_path, 'consolidated.csv')
        consolidated_df = consolidated_df[consolidated_df['description'].notnull()]
        consolidated_df.to_csv(new_file_path, index = False)

        return new_file_path

    indeed_jobs = extract_indeed_jobs()
    indeed_jobs = clean_indeed_jobs(indeed_jobs)

    internsg_jobs = extract_internsg_jobs()
    jobstreet_jobs = extract_jobstreet_jobs()

    mycareerfuture_jobs =  extract_mycareerfuture_jobs()
    mycareerfuture_jobs_2 =  extract_mycareerfuture_jobs_2()
    mycareerfuture_jobs_3 =  extract_mycareerfuture_jobs_3()
    mycareerfuture_jobs_4 =  extract_mycareerfuture_jobs_4()

    linkedin_jobs = extract_linkedin_jobs()
    linkedin_jobs_2 = extract_linkedin_jobs_2()
    linkedin_jobs_3 = extract_linkedin_jobs_3()

    new_indeed_jobs = rename_and_filter_columns(indeed_jobs)
    new_internsg_jobs =  rename_and_filter_columns(internsg_jobs)
    new_jobstreet_jobs = rename_and_filter_columns(jobstreet_jobs)
    new_mycareerfuture_1 = rename_and_filter_columns(mycareerfuture_jobs)
    new_mycareerfuture_2 = rename_and_filter_columns(mycareerfuture_jobs_2)
    new_mycareerfuture_3 = rename_and_filter_columns(mycareerfuture_jobs_3)
    new_mycareerfuture_4 = rename_and_filter_columns(mycareerfuture_jobs_4)
    new_linkedin_1 = rename_and_filter_columns(linkedin_jobs)
    new_linkedin_2 = rename_and_filter_columns(linkedin_jobs_2)
    new_linkedin_3 = rename_and_filter_columns(linkedin_jobs_3)

    consolidated_jobs = consolidate_files(new_indeed_jobs, new_internsg_jobs, new_jobstreet_jobs, new_mycareerfuture_1, new_mycareerfuture_2, new_mycareerfuture_3, new_mycareerfuture_4, new_linkedin_1, new_linkedin_2, new_linkedin_3)

dag = extract_transform_load()
