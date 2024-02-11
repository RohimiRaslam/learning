from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = ['GDP_USD_millions', 'Country']
db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP'
csv_path = 'Countries_by_GDP.csv'

# Function to extract the information from website and save to a dataframe
def extract(url, table_attribs):
    page = requests.get(url)                      #extract web page as text
    data = BeautifulSoup(page.text, 'html.parser')     #parse the text to html object
    df = pd.DataFrame(columns = table_attribs)    #create empty pandas dataframe
    tables = data.find_all('tbody')               #extract tbody attribute from html
    rows = tables[2].find_all('tr')               #extract all the rows of index 2
    for row in rows:                              #iterate through each rows
        col = row.find_all('td')                  #extract all data in the row
        if len(col)!=0:                           #check if the row is note empty
            if col[0].find('a') is not None and '—' not in col[2]:      #check if the first column contains hyperlink and if the third column contain '-'
                data_dict = {"Country": col[0].a.contents[0], "GDP_USD_millions": col[2].contents[0]}    #store the extracted data into a dictionary
                df1 = pd.DataFrame(data_dict, index=[0])                #append all dictionaries into the dataframe one by one
                df = pd.concat([df,df1], ignore_index=True)             #

    return df

#Function to transform the extracted information 
def transform(df):
    GDP_list = df["GDP_USD_millions"].tolist()                      #convert a given array to an ordinary list
    GDP_list = [float("".join(x.split(','))) for x in GDP_list]     #convert the currency to floating number
    GDP_list = [np.round(x/1000,2) for x in GDP_list]               #change the value's prefix to billion and round to 2 decimal places
    df["GDP_USD_millions"] = GDP_list                               #assign the modified list to the dataframe
    df=df.rename(columns = {"GDP_USD_millions":"GDP_USD_billions"}) #modify the name of the column in the dataframe from million to billion
    return df

#Save dataframe to csv
def load_to_csv(df, csv_path):
    df.to_csv(csv_path)
    
#Save final dataframe to database
def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    
#Query database table
def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

#Logging process
def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S'      # define the format of timestamp Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now()                        # get current timestamp 
    timestamp = now.strftime(timestamp_format)  # get the current time in the current timestamp format
    with open("./etl_project_log.txt","a") as f:    # write the timestamp into the log file
        f.write(timestamp + ' : ' + message + '\n')

#Log progresses        
log_progress('Preliminaries complete. Initiating ETL process')
df = extract(url, table_attribs)
log_progress('Data extraction complete. Initiating Transformation process')
df = transform(df)
log_progress('Data transformation complete. Initiating loading process')
load_to_csv(df, csv_path)
log_progress('Data saved to CSV file')
sql_connection = sqlite3.connect('World_Economies.db')
log_progress('SQL Connection initiated.')
load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as table. Running the query')
query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"
run_query(query_statement, sql_connection)
log_progress('Process Complete. \n')
sql_connection.close()