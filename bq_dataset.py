from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import os
def create_dataset(dataset_id):
    client=bigquery.Client(project='keshanna-123')
    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
        print(f'{dataset_ref} is already exists')

    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = 'US'
        dataset = client.create_dataset(dataset)
        print(f'the dataset {dataset_ref} is created sucessfully.')

dataset_id='gcs_airflow_dataset5'
create_dataset(dataset_id)        
