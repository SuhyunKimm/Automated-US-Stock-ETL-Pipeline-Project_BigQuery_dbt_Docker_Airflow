import pandas as pd
import numpy as np
from google.cloud import bigquery
from datetime import datetime
import upsert_etl_last_loaded

# load us market holidays data from csv file
df = pd.read_csv('../../resources/nyse_holidays_latest.csv')
df = df[df['date'] >= '2021-01-01']
df = df[['date', 'status', 'start_time', 'end_time', 'holiday_name']]

# rename columns
df.rename(columns = {
    "date" : "date",
    "status" : "status",
    "start_time" : "startTime",
    "end_time" : "endTime",
    "holiday_name" : "description"},
    inplace = True)

# format date column
df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')

# get the start and end date of the data
start_date = min(df['date'])
end_date = max(df['date'])

end_date_fmt = datetime.strptime(end_date, '%Y-%m-%d').date()

# Convert time columns safely
df['startTime'] = pd.to_datetime(df['startTime'], format='%H:%M', errors='coerce').dt.strftime('%H:%M:%S')
df['endTime']   = pd.to_datetime(df['endTime'], format='%H:%M', errors='coerce').dt.strftime('%H:%M:%S')

# BigQuery client
client = bigquery.Client(project='us-stock-project-487701')
table_id = 'us-stock-project-487701.us_stock.bronze_us_market_holidays'

# load data into BigQuery
job = client.load_table_from_dataframe(
    df,
    table_id,
    job_config = bigquery.LoadJobConfig(
        write_disposition='WRITE_APPEND'
    )
)

job.result()

# update etl_last_loaded
upsert_etl_last_loaded.upsert_etl_last_loaded("us_market_holidays", end_date_fmt)