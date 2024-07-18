import airflow
from  airflow import DAG
from google.cloud import storage
from google.cloud import bigquery
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime,timedelta

from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
import os

from google.cloud.exceptions import NotFound
default_args = {
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10)
}
dag=DAG(
    'gcp_airflow1',
    default_args=default_args,
    description='A DAG to copy files within GCP',
    schedule_interval=None,
    start_date=datetime(2024, 7, 16),
    catchup=False,
)
t0=DummyOperator(
    task_id='start',
    dag=dag
)

def create_gcs_bucket(bucket_id):
    bucket_id=bucket_id
    client=storage.Client(project='keshanna-123')
    try:
        bucket=client.get_bucket(bucket_id)
        print(f'the specified bucket {bucket_id} is already existed.')

    except NotFound:
        bucket=client.create_bucket(bucket_id)
        print(f'the specified bucket {bucket_id} is created.')


t1=PythonOperator(
    task_id='create_bucket',
    python_callable=create_gcs_bucket,
    op_kwargs={'bucket_id':'kesh_bucket'},
    dag=dag
) 


t2=GCSToGCSOperator(
    task_id='copy_file',
    source_bucket='us-central1-my-co-145e8db1-bucket',
    source_object='UNINVESTMENT.csv',
    destination_bucket='kesh_bucket',
    destination_object='UNINVESTMENT.csv',
    dag=dag
)

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

t3=PythonOperator(
    task_id='creatig_dataset',
    python_callable=create_dataset,
    op_kwargs={'dataset_id':'gcp_dataset'},
    dag=dag
)

def create_bqtable(dataset,table):
    client=bigquery.Client()
    dataset=client.get_dataset(dataset)
    table=dataset.table(table)
    try:
        table=client.get_table(table)
        print(f'{table} is already exists.')
    except NotFound:
        table=bigquery.Table(table)
        table=client.create_table(table)
        print(f'{table} is created.')    

t4=PythonOperator(
    task_id='create_bqtable',
    python_callable=create_bqtable,
    op_kwargs={'dataset':'gcp_dataset','table':'uni_investment'},
    dag=dag
)  

def load_data_from_gcs_to_bigquery(dataset_id, table_id, gcs_uri):
    client = bigquery.Client()

  
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        max_bad_records=1000,
        ignore_unknown_values=True,
        autodetect=True
    )
  
    table_ref = client.dataset(dataset_id).table(table_id)

    try:
        load_job = client.load_table_from_uri(
            gcs_uri, table_ref, job_config=job_config
        )

        print(f"Starting job {load_job.job_id}")

        
        load_job.result()  

        print(f"Job {load_job.job_id} completed.")

      
        if load_job.errors:
            print("Error loading data:")
            for error in load_job.errors:
                print(error["message"])
               
        else:
            print(f"Loaded {load_job.output_rows} rows into {dataset_id}.{table_id} from {gcs_uri}")

    except GoogleCloudError as e:
        print(f"Encountered an error: {e}")
        return

t5=PythonOperator(
    task_id='load_data_from_gcs_to_bigquery',
    python_callable=load_data_from_gcs_to_bigquery,
    op_kwargs={'dataset_id':'gcp_dataset','table_id':'uni_investment','gcs_uri':'gs://us-central1-my-co-145e8db1-bucket/UNINVESTMENT.csv'},
    dag=dag
)        



        
t6=DummyOperator(
    task_id='end',
    dag=dag
)
t0>>t1>>t2>>t3>>t4>>t5>>t6

