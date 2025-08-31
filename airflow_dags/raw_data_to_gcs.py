import csv
from google.cloud import storage

def mask_balance(input_file, output_file):
    with open(input_file, mode='r') as infile, open(output_file, mode='w', newline='') as outfile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            row['balance'] = '*****'
            writer.writerow(row)

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client(project="my-spark-project-463102")
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(f'File {source_file_name} uploaded to {destination_blob_name} in {bucket_name}.')


bucket_name = 'gcs-data-marketing-463102'
source_file_name = '/home/airflow/gcs/dags/scripts/bank_data.csv'
masked_file = 'masked_bank_data.csv'
destination_blob_name = 'raw_data/bank_data.csv'

mask_balance(source_file_name, masked_file)

upload_to_gcs(bucket_name, masked_file, destination_blob_name)