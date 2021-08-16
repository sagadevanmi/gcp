#!/usr/bin/env python3.7
from google.cloud import storage
from google.cloud import bigquery
from io import StringIO
import pandas as pd

client = storage.Client()
bucket = client.get_bucket("raw-dataset-group3")
blob = bucket.get_blob("Final_Assignment_Dataset_DE-2021-Batch1-GCP-Final-Assignment-Dataset.csv")

bt = blob.download_as_string()

s = str(bt, "utf-8")
s = StringIO(s)
df = pd.read_csv(s)


df['timestamp_local_converted'] = pd.to_datetime(df['unix_timestamp_local'],unit='s')

df['timestamp_utc_converted'] = pd.to_datetime(df['unix_timestamp_utc'],unit='s')

df.to_csv('Final_Assignment_Dataset_DE-2021-Batch1-GCP-Final-Assignment-Dataset.csv')

df.drop_duplicates(subset=None, inplace=True)
df.to_csv('Final_Assignment_Dataset_DE-2021-Batch1-GCP-Final-Assignment-Dataset.csv', index=False)


df = pd.read_csv('Final_Assignment_Dataset_DE-2021-Batch1-GCP-Final-Assignment-Dataset.csv')

for i in set(df['date_local']):
    df.loc[df['date_local'] == i].to_csv(f"{i}.csv",index=False)

bucket = client.get_bucket('processed-dataset-group3')
blob = bucket.blob('02-03-2019.csv')
blob1 = bucket.blob('03-03-2019.csv')
blob2 = bucket.blob('04-03-2019.csv')
blob3 = bucket.blob('05-03-2019.csv')
blob4 = bucket.blob('06-03-2019.csv')

blob.upload_from_filename('02-03-2019.csv')
blob1.upload_from_filename('03-03-2019.csv')
blob2.upload_from_filename('04-03-2019.csv')
blob3.upload_from_filename('05-03-2019.csv')
blob4.upload_from_filename('06-03-2019.csv')

client = bigquery.Client()
project = client.project
dataset_ref = bigquery.DatasetReference(project, 'group3')
table_ref = dataset_ref.table("processed-dataset-group3")
schema = [
    bigquery.SchemaField("visitor_device_key", "INTEGER"),
    bigquery.SchemaField("visitor_device_Id", "STRING"),
    bigquery.SchemaField("location_key", "INTEGER"),
    bigquery.SchemaField("visit_dwell_time", "INTEGER"),
    bigquery.SchemaField("dma_code", "INTEGER"),
    bigquery.SchemaField("dma", "STRING"),
    bigquery.SchemaField("zip_code", "INTEGER"),
    bigquery.SchemaField("state", "STRING"),
    bigquery.SchemaField("timezone", "STRING"),
    bigquery.SchemaField("brand", "STRING"),
    bigquery.SchemaField("category", "STRING"),
    bigquery.SchemaField("unix_timestamp_local", "INTEGER"),
    bigquery.SchemaField("unix_timestamp_utc", "INTEGER"),
    bigquery.SchemaField("date_local", "DATE"),
    bigquery.SchemaField("timestamp_local_converted", "DATE")
    bigquery.SchemaField("timestamp_utc_converted", "DATE")
]
table = bigquery.Table(table_ref, schema=schema)
table.time_partitioning = bigquery.TimePartitioning(
    type_=bigquery.TimePartitioningType.DAY,
    field="date_local",  # name of column to use for partitioning
    expiration_ms=7776000000,
)  # 90 days

table = client.create_table(table)

print(
    "Created table {}, partitioned on column {}".format(
        table.table_id, table.time_partitioning.field
    )
)
