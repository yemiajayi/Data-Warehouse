from dwh_conn import *

# DROP TABLES

drop_table_staging_event = "DROP TABLE IF EXISTS staging_event;"
drop_table_staging_song = "DROP TABLE IF EXISTS staging_song;"
drop_table_songplays = "DROP TABLE IF EXISTS songplays;"
drop_table_users = "DROP TABLE IF EXISTS users;"
drop_table_songs = "DROP TABLE IF EXISTS songs;"
drop_table_artists = "DROP TABLE IF EXISTS artists;"
drop_table_time = "DROP TABLE IF EXISTS time;"

#CREATE STAGING TABLES
set_staging_search_path = "SET search_path TO public;"

create_table_staging_event = ("""
CREATE TABLE IF NOT EXISTS staging_event(
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender TEXT,
    itemInSession INTEGER,
    lastName VARCHAR,
    length FLOAT,
    level TEXT,
    location VARCHAR,
    method TEXT,
    page TEXT,
    registration FLOAT,
    sessionId INTEGER,
    song VARCHAR,
    status INTEGER,
    ts BIGINT,
    userAgent VARCHAR,
    userId VARCHAR
    );""")

create_table_staging_song = ("""
CREATE TABLE IF NOT EXISTS staging_song(
    num_songs INTEGER,
    artist_id VARCHAR(100),
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR(100),
    artist_name VARCHAR(100),
    song_id VARCHAR(50),
    title VARCHAR(100),
    duration FLOAT,
    year INTEGER
    );""")

#CREATE SCHEMA FOR ANALYTIC TABLES

create_schema = "CREATE SCHEMA IF NOT EXISTS project;"
set_search_path = "SET search_path TO project;"

# CREATE ANALYTICS TABLES

create_table_songplays = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INTEGER IDENTITY(1, 1) PRIMARY KEY,
    start_time bigint NOT NULL REFERENCES time(start_time) sortkey,
    user_id VARCHAR NOT NULL REFERENCES users(user_id),
    level TEXT,
    song_id varchar(50) NOT NULL REFERENCES songs(song_id) distkey,
    artist_id varchar(100) NOT NULL REFERENCES artists(artist_id),
    session_id INTEGER NOT NULL,
    location varchar(200),
    user_agent varchar(225)
);
""")

create_table_users = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id varchar(50) PRIMARY KEY,
    first_name varchar(50),
    last_name varchar(50),
    gender TEXT,
    level TEXT
)
DISTSTYLE all;
""")

create_table_songs = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id varchar(50) PRIMARY KEY,
    title varchar(100),
    artist_id varchar(100) REFERENCES artists(artist_id) sortkey distkey,
    year INTEGER,
    duration FLOAT
);
""")

create_table_artists = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar(100) PRIMARY KEY,
    name varchar(100),
    location varchar(100),
    latitude float,
    longitude float
)
DISTSTYLE all;
""")


create_table_time = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time bigint PRIMARY KEY,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER
)
DISTSTYLE all;
""")

# INSERT RECORDS

insert_into_staging_event = ("""COPY staging_event FROM 's3://dwh-training/data/log_data/2018/11/'
    credentials 'aws_iam_role={}' region 'us-east-2' JSON '{}';""").format(DWH_ROLE_ARN, JSONPATH)

insert_into_staging_song = ("""COPY staging_song FROM 's3://dwh-training/data/song_data/A/'
    credentials 'aws_iam_role={}' region 'us-east-2' JSON 'auto';""").format(DWH_ROLE_ARN)

insert_into_songplays = ("""
INSERT INTO songplays (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent) 
SELECT SE.ts AS start_time,
SE.userId AS user_id,
SE.level,
SS.song_id,
SS.artist_id,
SE.sessionId AS session_id,
SE.location,
SE.userAgent AS user_agent
FROM public.staging_event SE
JOIN public.staging_song SS ON SE.song = SS.title
AND SE.artist = SS.artist_name
LEFT OUTER JOIN songplays ON SE.userId = songplays.user_id
AND SE.ts = songplays.start_time
WHERE SE.page = 'NextSong'
AND SE.userId IS NOT NULL
AND SE.level IS NOT NULL
AND SS.song_id IS NOT NULL
AND SS.artist_id IS NOT NULL
AND SE.sessionId IS NOT NULL
AND SE.location IS NOT NULL
AND SE.userAgent IS NOT NULL
ORDER BY start_time, user_id
""")

insert_into_users = ("""
INSERT INTO users (
    user_id,
    first_name,
    last_name,
    gender,
    level) 
SELECT userId AS user_id, firstName AS first_name,
lastName AS last_name, gender, level
FROM public.staging_event
WHERE userId IS NOT NULL
ORDER BY user_id;
""")

insert_into_songs = ("""
INSERT INTO songs (
    song_id,
    title,
    artist_id,
    year,
    duration) 
SELECT song_id, title, artist_id, year, duration
FROM public.staging_song
ORDER BY song_id;
""")

insert_into_artists = ("""
INSERT INTO artists (
    artist_id,
    name,
    location,
    latitude,
    longitude)
SELECT artist_id, artist_name, artist_location AS location,
artist_latitude AS latitude, artist_longitude AS longitude
FROM public.staging_song
WHERE artist_name IS NOT NULL
ORDER BY artist_id;
""")


insert_into_time = ("""
INSERT INTO time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday)
SELECT start_time,
    EXTRACT(hour FROM date_time) AS hour,
    EXTRACT(day FROM date_time) AS day,
    EXTRACT(week FROM date_time) AS week,
    EXTRACT(month FROM date_time) AS month,
    EXTRACT(year FROM date_time) AS year,
    EXTRACT(weekday FROM date_time) AS weekday
FROM (SELECT ts AS start_time,
    CAST('1900-01-01' AS DATE) + ts/1000 * interval '1 sec' AS date_time
    FROM public.staging_event)
ORDER BY start_time;
""")

# QUERY LISTS

create_staging_table_queries = [set_staging_search_path, drop_table_staging_event, drop_table_staging_song, create_table_staging_event, create_table_staging_song]
insert_staging_table_queries = [set_staging_search_path, insert_into_staging_event, insert_into_staging_song]

schema_queries = [create_schema, set_search_path]
#
drop_table_queries = [drop_table_songplays, drop_table_users, drop_table_songs, drop_table_artists, drop_table_time]
#
create_table_queries = [create_table_users, create_table_artists, create_table_songs, create_table_time, create_table_songplays]
#
insert_table_queries = [set_search_path, insert_into_users, insert_into_songs, insert_into_artists, insert_into_time, insert_into_songplays]