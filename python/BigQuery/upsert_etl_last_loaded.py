from google.cloud import bigquery
import pandas as pd

def upsert_etl_last_loaded(table_name: str, last_loaded_date) :
    client = bigquery.Client(project='us-stock-project-487701')

    merge_query = f"""
        MERGE `us-stock-project-487701.us_stock.etl_last_loaded` T
        USING (SELECT '{table_name}' AS table_name, DATE('{last_loaded_date}') AS last_loaded_date) S
        ON T.table_name = S.table_name
        WHEN MATCHED THEN
        UPDATE SET last_loaded_date = S.last_loaded_date
        WHEN NOT MATCHED THEN
        INSERT (table_name, last_loaded_date)
        VALUES(S.table_name, S.last_loaded_date)
    """
    client.query(merge_query).result()