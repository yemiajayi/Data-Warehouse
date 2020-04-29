import numpy as np
import psycopg2
import pandas as pd
import boto3
from db import cur
import create_tables
from sql_queries import insert_staging_table_queries, insert_table_queries
from dwh_conn import *

#create the tables
create_tables.create_tables()

#connect to the S3 bucket for datasets
s3 = boto3.resource('s3',
                   region_name='us-east-2',
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET)

DbBucket = s3.Bucket("dwh-training")

"""
#view data files
for song_obj in DbBucket.objects.filter(Prefix="song_data/A/"):
    print(song_obj)

for log_obj in DbBucket.objects.filter(Prefix="log_data/2018/"):
    print(log_obj)
"""

#load data into staging tables
print("populating staging table...")
for query in insert_staging_table_queries:
    cur.execute(query)

print("copying into staging_event and staging_song completed")

#insert into fact and dimenssion tables
print("inserting into analytic tables...")
for query in insert_table_queries:
	cur.execute(query)

print("insert job complete")


#process log files