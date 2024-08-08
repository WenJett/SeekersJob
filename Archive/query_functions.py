# import packages
import pypyodbc as odbc 
import pandas as pd
from credentials import server, database, username, password

# before defining please import pyodbc, pandas, from credentials import server, database, username, password

# initialize database and insert data query
# can update file path here 
file_path = '/Users/Jason/Desktop/IS3107/is3107_DataEngineering/consolidated.csv'
def insert_into_database(file_path=file_path, server=server, database=database, username=username, password=password):
    try:
        connection_string = 'Driver={ODBC Driver 18 for SQL Server};Server='+server+',1433;Database='+database+';Uid='+username+';Pwd='+password+';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
        
        # connect to database 
        conn = odbc.connect(connection_string)
        cursor = conn.cursor()

        # load df 
        df = pd.read_csv(file_path, encoding='utf-8')
        df = df.fillna('NULL VALUES')
        df.columns = ['job_title', 'description', 'company', 'salary_range']
        
        # delete table if present 
        delete_query = """
        IF OBJECT_ID('is3107JobsDB.dbo.consolidatedJobs', 'U') IS NOT NULL
        BEGIN
            DROP TABLE is3107JobsDB.dbo.consolidatedJobs;
        END
        """
        cursor.execute(delete_query)
        
        # recreate table 
        create_query = 'CREATE TABLE consolidatedJobs (job_title VARCHAR(MAX) NULL, description VARCHAR(MAX) NULL, company VARCHAR(MAX) NULL, salary_range VARCHAR(MAX) NULL)'
        cursor.execute(create_query)
        
        # upload data onto the database 
        load_data_query = """
                   INSERT INTO is3107JobsDB.dbo.consolidatedJobs (job_title, description, company, salary_range)
                   VALUES (?, ?, ?, ?)
                   """
        for row in df.itertuples():
            cursor.execute(load_data_query,
                   (row.job_title,
                   row.description, 
                   row.company,
                   row.salary_range))
        conn.commit()
        return "Database Loaded Successfully"
    except:
        return "Error encountered"

# select query
# can update select query here
select_all_query = 'SELECT * FROM consolidatedJobs'
def query_data(server=server, database=database, username=username, password=password, query=select_all_query):
    try:
        connection_string = 'Driver={ODBC Driver 18 for SQL Server};Server='+server+',1433;Database='+database+';Uid='+username+';Pwd='+password+';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
        
        # connect to database 
        conn = odbc.connect(connection_string)
        cursor = conn.cursor()

        # execute select query 
        cursor.execute(query)
        dataset = cursor.fetchall()

        # return the dataset
        colnames = ['job_title', 'description', 'company', 'salary_range']
        return pd.DataFrame(dataset, columns=colnames)
    except:
        return pd.DataFrame()
