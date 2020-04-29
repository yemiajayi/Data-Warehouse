from dwh_conn import *
import psycopg2

"""def connect():
	conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(DWH_ENDPOINT, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT))
	conn.set_session(autocommit=True)
	print("connecting to Redshift database...")
	cur = conn.cursor()
	return cur, conn"""



conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(DWH_ENDPOINT, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT))
conn.set_session(autocommit=True)
print("connecting to Redshift database...")
cur = conn.cursor()
