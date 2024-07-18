from google.cloud import storage
import os
from google.cloud.exceptions import NotFound

def create_gcs_bucket(bucket_id):
    bucket_id=bucket_id
    client=storage.Client(project='keshanna-123')
    try:
        bucket=storage.Client.get_bucket(bucket_id)
        print(f'the specified bucket {bucket_id} is already existed.')

    except NotFound:
        bucket=client.create_bucket(bucket_id)
        print(f'the specified bucket {bucket_id} is created.')

bucket_id='jul-19'
create_gcs_bucket(bucket_id=bucket_id)