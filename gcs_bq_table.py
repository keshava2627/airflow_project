from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError
import os


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

project_id = "keshanna-123"
dataset_id = "gcs_airflow_dataset"
table_id = "UNINVESTMENT3"
gcs_uri = "gs://jul-17/sales_use_case/UNINVESTMENT.csv"
load_data_from_gcs_to_bigquery(dataset_id, table_id, gcs_uri)
