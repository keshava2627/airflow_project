from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import os

def create_bqtable(client,dataset,table):
    dataset=client.get_dataset(dataset)
    table=dataset.table(table)
    try:
        table=client.get_table(table)
        print(f'{table} is already exists.')
    except NotFound:
        table=bigquery.Table(table)
        table=client.create_table(table)
        print(f'{table} is created.')    

client=bigquery.Client()
dataset='gcs_airflow_dataset1'
table='UNINVESTMENT3'
create_bqtable(client,dataset,table)         