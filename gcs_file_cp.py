from google.cloud import storage
from google.cloud.exceptions import NotFound
import os

def copy_file_gcs(bucket_id,source_file,destiation_file):
    client=storage.Client(project='keshanna-123')
    bucket=client.bucket(bucket_id)
    destination_blob=bucket.blob(destiation_file)
    with open(source_file,'rb')as file:
        destination_blob.upload_from_file(file)

    print(f'{source_file} is sucessfully copied to {destiation_file} in the {bucket_id} bucket')


bucket_id='jul-18'
source_file="C:/Users/kesha/Downloads/UNINVESTMENT.csv"
destiation_file='sales_use_case/UNINVESTMENT.csv'
   