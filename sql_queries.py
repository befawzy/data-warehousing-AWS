import configparser

# CONFIG
config = configparser.ConfigParser()
config.read("dwh.ini")
IAM = config.get("IAM_ROLE", "ROLE_NAME")
LOG_DATA_LOCATION = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA_LOCATION = config.get("S3", "SONG_DATA")
UDACITY_DATA_REGION = config.get("S3", "UDACITY_DATA_REGION")


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
users_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = """
CREATE TABLE IF NOT EXISTS staging_events
(
artist          VARCHAR(MAX),
auth            VARCHAR(MAX),
first_name      VARCHAR(MAX),
gender          VARCHAR(MAX),
item_in_session INTEGER,
last_name       VARCHAR(MAX),
length          FLOAT,
level           VARCHAR(MAX),
location        VARCHAR(MAX),
method          VARCHAR(MAX),
page            VARCHAR(MAX),
registration    FLOAT,
session_id      INTEGER,
song            VARCHAR(MAX),
status          INTEGER,
ts              INTEGER,
user_agent      VARCHAR(MAX),
user_id         VARCHAR(MAX)                                     
);
"""
staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS staging_song
(
artist_id        VARCHAR(MAX),
artist_latitude  FLOAT,
artist_location  VARCHAR(MAX),
artist_longitude FLOAT,
artist_name      VARCHAR(MAX),
duration         FLOAT,
num_songs        INTEGER,
song_id          VARCHAR(MAX),
title            VARCHAR(MAX),
year             INTEGER
);
"""

songplay_table_create = """CREATE TABLE IF NOT EXISTS songplay
(
 songplay_id    VARCHAR(MAX) PRIMARY KEY,
 start_time     TIMESTAMP REFERENCES time(start_time) NOT NULL,
 user_id        VARCHAR(MAX) REFERENCES users(user_id) NOT NULL,
 level          VARCHAR(MAX),
 song_id        VARCHAR(MAX) REFERENCES song(song_id) NOT NULL,
 artist_id      VARCHAR(MAX) REFERENCES artist(artist_id) NOT NULL,
 session_id     INTEGER,
 location       VARCHAR(MAX),
 user_agent     VARCHAR(MAX)
)
DISTKEY(songplay_id)
SORTKEY(start_time)
;
"""

users_table_create = """CREATE TABLE IF NOT EXISTS users 
(
 user_id        VARCHAR(MAX) PRIMARY KEY,
 first_name     VARCHAR(MAX),
 last_name      VARCHAR(MAX),
 gender         VARCHAR(MAX),
 level          VARCHAR(MAX),
 location       VARCHAR(MAX),
 user_agent     VARCHAR(MAX)
)
DISTKEY(user_id)
SORTKEY(user_id);
"""
# song_id, title, artist_id, year, duration
song_table_create = """CREATE TABLE IF NOT EXISTS song 
(
 song_id        VARCHAR(MAX) PRIMARY KEY,
 title          VARCHAR(MAX),
 artist_id      VARCHAR(MAX),
 year           INTEGER,
 duration       FLOAT
)
DISTKEY(song_id)
SORTKEY(duration);
;
"""

artist_table_create = """CREATE TABLE IF NOT EXISTS artist 
(
 artist_id         VARCHAR(MAX) PRIMARY KEY,
 artist_name       VARCHAR(MAX),
 artist_location   VARCHAR(MAX),
 artist_latitude   FLOAT,
 artist_logitude   FLOAT
)
DISTKEY(artist_id)
SORTKEY(artist_id);
"""

time_table_create = """CREATE TABLE IF NOT EXISTS time
(
 start_time     TIMESTAMP PRIMARY KEY,
 hour           INTEGER,
 day            INTEGER,
 week           INTEGER,
 month          INTEGER,
 year           INTEGER,
 weekday        INTEGER
)
DISTKEY(start_time)
SORTKEY(start_time, year, month, week, day, weekday, hour);
"""


# STAGING TABLES

staging_events_copy = (
    """
COPY staging_events from '{}'
iam_role '{}'
json '{}'
"""
).format(LOG_DATA_LOCATION, IAM, LOG_JSONPATH)

staging_songs_copy = (
    """
COPY staging_songs from '{}'
iam_role '{}'
json 'auto'
"""
).format(SONG_DATA_LOCATION, IAM)

# FINAL TABLES (Star Schema)
songplay_table_insert = """INSERT INTO songplay 
(
SELECT CONCAT(song_id,session_id,item_in_session) as songplay_id, to_timestamp(ts/1000.0) as start_time,
 user_id, level, song_id, artist_id, session_id, artist_location, user_agent
 FROM staging_events
 JOIN staging_songs
 ON (staging_events.song = staging_songs.title and staging_events.artist = staging_songs.artist_name)
 WHERE staging_events.page = 'NextSong'
);
"""

users_table_insert = """INSERT INTO users
(
SELECT user_id, first_name, last_name, gender, level
FROM staging_events WHERE user_id NOT IN (SELECT user_id FROM user)
);
"""

# song_id, title, artist_id, year, duration
song_table_insert = """INSERT INTO song
(
SELECT song_id, title, artist_id, year, duration
FROM staging_song WHERE song_id NOT IN (SELECT song_id FROM song)
);
"""

artist_table_insert = """
INSERT INTO artist
(
SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_song WHERE artist_id NOT IN (SELECT artist_id FROM artist)
);
"""
# start_time, hour, day, week, month, year, weekday
time_table_insert = """
INSERT INTO time
(
WITH initial_time AS(
    SELECT DATEADD('ms', ts, '19700101') as start_time
    FROM staging_events
    GROUP BY 1
)

SELECT  distinct to_timestamp(ts/1000.0) as start_time,
        EXTRACT(HOUR FROM to_timestamp(ts/1000.0)) AS hour,
        EXTRACT(DAY FROM to_timestamp(ts/1000.0)) AS day, 
        EXTRACT(WEEK FROM to_timestamp(ts/1000.0)) AS week,
        EXTRACT(MONTH to_timestamp(ts/1000.0)) AS month,
        EXTRACT(YEAR FROM to_timestamp(ts/1000.0)) AS year,
        EXTRACT(DOW FROM to_timestamp(ts/1000.0)) AS weekday,
FROM staging_events WHERE start_time NOT IN (SELECT start_time FROM initial_time)
);                      
"""

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    users_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
    songplay_table_create,
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    users_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    songplay_table_insert,
    users_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]
