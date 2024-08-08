from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import psycopg2
# import os
# os.environ['no_proxy'] = '*'
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy
from airflow.models import TaskInstance
from airflow.utils.state import State

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 12, 4),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10)
}

# def function to scrape internsg 
def internSgScrapper(pages=39, output_file_path='internSG_jobs.csv'):
    # get all pages first 
    list_of_all_pages = []
    for i in range(1,pages):
        url = f'https://www.internsg.com/jobs/{i}/?f_p=107&f_i&filter_s#isg-top'
        list_of_all_pages.append(url)

    # get title of all jobs 
    data = []
    for url in list_of_all_pages:
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        divs = soup.find_all('div', class_='ast-col-lg-3')
        for div in divs:
            # Try to find an <a> tag within the <div>
            a_tag = div.find('a')
            # If an <a> tag is found and it has a 'href' attribute
            if a_tag and 'href' in a_tag.attrs:
                # Extract the URL and the job title
                url = a_tag['href']
                job_title = a_tag.get_text().strip()
                # Append the data to the list
                date = 'Not Available'
                # Try to find the next sibling 'div' which might contain the date
                date_div = div.find_next_sibling('div', class_='ast-col-lg-1')
                if date_div:
                    date_span = date_div.find('span', class_='text-monospace')
                    if date_span:
                        date = date_span.get_text().strip()
                data.append({'URL': url, 'Job Title': job_title, 'Date':date})

    title_url_df = pd.DataFrame(data)
    col_names = ['Company','Designation','Date Listed','Job Type','Job Period','Profession',
             'Industry','Location Name','Allowance / Remuneration','Company Profile',
             'Job Description']
    jobs_info = []
    for url in title_url_df['URL']:
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        res_dict = dict.fromkeys(col_names, '')
        for col_name in col_names:
            # Find the div that contains the column name
            col_div = soup.find('div', text=col_name, class_='font-weight-bold')
            if col_div:
                # The actual data is in the next sibling of the parent of col_div
                next_div = col_div.find_next_sibling()
                if next_div:
                    # Extract the text and store it in the dictionary
                    for span in next_div.find_all('span'):
                        span.decompose()
                    res_dict[col_name] = next_div.get_text(strip=True)
        jobs_info.append(res_dict)
    
    # return results 
    internSG_jobs = pd.DataFrame(jobs_info)
    internSG_jobs.to_csv(output_file_path,index=False)
    return internSG_jobs

def load_csv(consolidated_file_path='consolidated.csv'):
    try:
        df = pd.read_csv(consolidated_file_path)
        df = df.fillna("Null")
        return df
    except:
        return pd.DataFrame({'job_title':['test1'], 'description':['test1'], 'company':['test1'], 'salary_range':['test1']})
     

@dag(dag_id='create_postgres_table', default_args=default_args, schedule=None, catchup=False, tags=['create table'])
def test_create_table():
    @task
    def remove_table_if_present_table():
        create_table_sql = """
        DROP TABLE consolidatedJobs;
        """
        # DO $$ 
        # BEGIN
        #     IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'is3107JobsDB' AND table_name = 'consolidatedJobs') THEN
        #     DROP TABLE consolidatedJobs;
        #     END IF;
        # END $$;
        # Use the Airflow PostgresHook to execute the SQL
        hook = PostgresHook(postgres_conn_id='postgres_azurehost')
        hook.run(create_table_sql, autocommit=True)
    
    @task
    def create_table():
        create_table_sql = """
        CREATE TABLE consolidatedJobs (
            job_title TEXT,
            description TEXT,
            company TEXT,
            salary_range TEXT,
            url Text
        );
        """
        # Use the Airflow PostgresHook to execute the SQL
        hook = PostgresHook(postgres_conn_id='postgres_azurehost')
        hook.run(create_table_sql, autocommit=True)
        
    @task 
    def upload_data():
        postgres_hook = PostgresHook(postgres_conn_id="postgres_azurehost")
        df = load_csv()
        truncate_sql = """TRUNCATE TABLE table_name;"""
        postgres_hook.run(truncate_sql, autocommit=True)
        load_sql = """
        INSERT INTO consolidatedJobs (job_title, description, company, salary_range, url)
        VALUES (%(job_title)s, %(description)s, %(company)s, %(salary_range)s, %(url)s);
        """
        for idx, row in df.head().iterrows():
            row_entry = {
            'job_title': row['job title'],
            'description': row['description'],
            'company': row['company'],
            'salary_range': row['salary_range'],
            'url': row['url']
            }
            # Use the Airflow PostgresHook to execute the SQL
            postgres_hook.run(load_sql, parameters=row_entry, autocommit=True)
        
    @task 
    def load_data(load_file_path='test.csv'):
        select_table_query = """SELECT * FROM consolidatedJobs"""
        conn = psycopg2.connect(database="is3107JobsDB", user='is3107Postgres', password='JobsDBProject3107', host='is3107.postgres.database.azure.com', port= '5432')
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(select_table_query)
        result = cursor.fetchall()
        print(result)
        conn.commit()
        conn.close()        
        colnames = ['job_title', 'description', 'company', 'salary_range']
        pd.DataFrame(result, columns=colnames).to_csv(load_file_path)
        return pd.DataFrame(result, columns=colnames)
    
    @task
    def internSg_scrape(**context):
        result = internSgScrapper()
        print('The scraped data has {} rows'.format(len(result)))

    # task dependencies are defined in a straightforward way
    # remove_table_if_present_table()
    # create_table()
    upload_data()
    # load_data()
    internSg_scrape()

create_table_dag = test_create_table()