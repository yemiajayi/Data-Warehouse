from db import *
from sql_queries import *

#connect to redshift db
#db.connect()

def create_tables():
    #create staging tables
    for query in create_staging_table_queries:
        cur.execute(query)

    #create schema for analytic tables and set as search path
    for query in schema_queries:
        cur.execute(query)
    print("Schema created and set as search_path")

    #first drop table queries
    for query in drop_table_queries:
        cur.execute(query)
    print("tables dropped")

    #run create table queries
    print("creating analytic tables")
    for query in create_table_queries:
        print(query)
        cur.execute(query)
    print("analytic tables created")