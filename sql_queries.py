
import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE = config.get('IAM_ROLE', 'ARN')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')

# DROP TABLES
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"


# CREATE TABLES
staging_events_table_create= ("""
CREATE TABLE staging_events (
    artist text,
    auth text not null,
    firstName text,
    gender char(1),
    itemInSession integer,
    lastName text,
    length numeric(12, 8),
    level text not null,
    location text,
    method text not null,
    page text not null,
    registration numeric,
    sessionId integer,
    song text,
    status smallint,
    ts bigint not null,
    userAgent text,
    userId int
)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    artist_id text not null,
    artist_location text,
    artist_longitude text,
    artist_latitude text,
    artist_name text not null,
    duration numeric(12, 8) not null,
    num_songs smallint,
    song_id text not null,
    title text not null,
    year smallint
)
""")

songplay_table_create = ("""CREATE TABLE songplays (
songplay_id integer IDENTITY(0,1) primary key,
start_time bigint not null,
user_id integer not null,
level text not null,
song_id char(18),
artist_id char(18),
session_id text not null,
location text not null,
user_agent text not null)""")

user_table_create = ("""CREATE TABLE users (
user_id integer primary key,
first_name text not null,
last_name text not null,
gender char(1) not null,
level text not null)
""")

song_table_create = (""" CREATE TABLE songs (
song_id char(18) primary key,
title text not null,
artist_id text not null,
year smallint not null,
duration numeric(12, 8) not null)
""")

artist_table_create = (""" CREATE TABLE artists (
artist_id char(18) primary key,
name text not null,
location text,
latitude numeric(12, 8),
longitude numeric(12, 8))
""")

time_table_create = (""" CREATE TABLE time (
start_time bigint not null,
hour smallint not null,
day smallint not null,
week smallint not null,
month smallint not null,
year smallint not null,
weekday boolean not null)
""")

# STAGING TABLES
staging_events_copy = ("""
    COPY staging_events FROM {}
    IAM_ROLE {}
    REGION 'us-west-2'
    JSON {}
""").format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    IAM_ROLE {}
    REGION 'us-west-2'
    JSON 'auto'
""").format(SONG_DATA, IAM_ROLE)

# FINAL TABLES
songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT ts AS start_time,
    se.userId AS user_id,
    se.level AS level,
    ss.song_id AS song_id,
    ss.artist_id AS artist_id,
    se.sessionId AS session_id,
    se.location AS location,
    se.userAgent AS user_agent
FROM staging_events AS se
JOIN staging_songs AS ss
ON (se.artist = ss.artist_name)
WHERE se.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT
    userId AS user_id,
    firstName AS first_name,
    lastName AS last_name,
    gender,
    level
FROM staging_events
WHERE page = 'NextSong' AND userId IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT song_id, title, artist_id, year, duration FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT ts AS start_time,
    EXTRACT(hour FROM (TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second'))    AS hour,
    EXTRACT(day FROM (TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second'))    AS day,
    EXTRACT(week FROM (TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second'))    AS week,
    EXTRACT(month FROM (TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second'))    AS month,
    EXTRACT(year FROM (TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second'))    AS year,
    EXTRACT(dow FROM (TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second'))    AS weekday
FROM staging_events WHERE page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
