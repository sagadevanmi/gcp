#!/usr/bin/env python3
from google.cloud import storage
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

bucket = client.get_bucket('raw-dataset-group3')
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

