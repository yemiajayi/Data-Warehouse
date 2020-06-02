import psycopg2
import boto3
from db import cur
import create_tables
from sql_queries import * #insert_staging_table_queries, insert_table_queries
from dwh_conn import *

#connect to redshift db
#db.connect()

#create the tables
create_tables.create_tables()

"""
#connect to the S3 bucket for datasets
#URLs: s3://udacity-dend/song_data; s3://udacity-dend/log_data
s3 = boto3.resource('s3',
                region_name='us-east-2',
                aws_access_key_id=KEY,
                aws_secret_access_key=SECRET)

DbBucket = s3.Bucket("dwh-training")


#view data files
for song_obj in DbBucket.objects.filter(Prefix="song_data/A/"):
    print(song_obj)

for log_obj in DbBucket.objects.filter(Prefix="log_data/2018/"):
    print(log_obj)
"""

#load data into staging tables
for query in insert_staging_table_queries:
    cur.execute(query)
print("staging event completed")

"""for query in insert_staging_song_batch1:
    cur.execute(query)
print("staging song batch 1 completed")

for query in insert_staging_song_batch2:
    cur.execute(query)
print("staging song batch 2 completed")

for query in insert_staging_song_batch3:
    cur.execute(query)
print("staging song batch 3 completed")

for query in insert_staging_song_batch4:
    cur.execute(query)
print("staging song batch 4 completed")

for query in insert_staging_song_batch5:
    cur.execute(query)
print("staging song batch 5 completed")

for query in insert_staging_song_batch6:
    cur.execute(query)
print("staging song batch 6 completed")

for query in insert_staging_song_batch7:
    cur.execute(query)
print("staging song batch 7 completed")

for query in insert_staging_song_batch8:
    cur.execute(query)
print("staging song batch 8 completed")

for query in insert_staging_song_batch9:
    cur.execute(query)
print("staging song batch 9 completed")"""

#insert into fact and dimenssion tables
print("inserting into analytic tables...")
query_numb = 0
for query in insert_table_queries:
	query_numb = query_numb + 1
	print(query_numb)
	cur.execute(query)

print("insert job complete")


#process log files