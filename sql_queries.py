import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ARN = config.get('IAM_ROLE', 'ARN')
LOG_DATA = config.get('S3', 'log_data')
SONG_DATA = config.get('S3', 'song_data')
LOG_JSONPATH = config.get('S3', 'log_jsonpath')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR,
    auth VARCHAR,
    first_name VARCHAR,
    gender VARCHAR,
    item_in_session INT,
    last_name VARCHAR,
    length DOUBLE PRECISION,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration BIGINT,
    session_id INT,
    song VARCHAR,
    status INT,
    ts BIGINT,
    user_agent VARCHAR,
    user_id VARCHAR)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    song_id VARCHAR,
    title VARCHAR,
    year INT,
    duration DOUBLE PRECISION,
    artist_id VARCHAR,
    artist_name VARCHAR,
    artist_location VARCHAR,
    artist_latitude DOUBLE PRECISION,
    artist_longitude DOUBLE PRECISION)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    user_id VARCHAR(255) NOT NULL,
    ts VARCHAR(255) NOT NULL distkey,
    level VARCHAR(10),
    song_id VARCHAR(255) NOT NULL,
    artist_id VARCHAR(255) NOT NULL,
    session_id VARCHAR(255),
    location VARCHAR(255),
    user_agent VARCHAR(500),
    songplay_id BIGINT IDENTITY(0,1) NOT NULL,
    PRIMARY KEY (songplay_id),
    FOREIGN KEY (song_id) REFERENCES songs(song_id),
    FOREIGN KEY (artist_id) REFERENCES artists(artist_id),
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY (ts) REFERENCES time(ts_raw))
    diststyle KEY;
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR(255) UNIQUE NOT NULL,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    gender VARCHAR(10),
    level VARCHAR(10),
    PRIMARY KEY (user_id))
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR(255) UNIQUE NOT NULL,
    title VARCHAR(255),
    artist_id VARCHAR(255) NOT NULL,
    year INT,
    duration DOUBLE PRECISION,
    PRIMARY KEY (song_id))
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR(255) UNIQUE NOT NULL,
    name VARCHAR(255),
    location VARCHAR(500),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    PRIMARY KEY (artist_id))
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    ts_raw BIGINT NOT NULL distkey,
    start_time TIMESTAMP,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday INT,
    PRIMARY KEY (ts_raw));
""")

# POPULATE STAGING TABLES
staging_events_copy = ("""
COPY staging_events
FROM {}
CREDENTIALS 'aws_iam_role={}'
JSON {};
""").format(LOG_DATA, IAM_ARN, LOG_JSONPATH)

staging_songs_copy = ("""
COPY staging_songs
FROM {}
CREDENTIALS 'aws_iam_role={}'
JSON 'auto';
""").format(SONG_DATA, IAM_ARN)

# FINAL TABLES
songplay_table_insert = ("""
INSERT INTO songplays
(SELECT
    ev.user_id,
    ev.ts,
    ev.level, 
    so.song_id, 
    so.artist_id, 
    ev.session_id, 
    ev.location,
    ev.user_agent
FROM staging_events ev
JOIN staging_songs so ON ev.song = so.title AND ev.artist = so.artist_name AND ev.length = so.duration
WHERE page = 'NextSong')
""")

user_table_insert = ("""
INSERT INTO users
(SELECT 
    distinct user_id, 
    first_name, 
    last_name, 
    gender,
    first_value(level) OVER (partition by user_id order by ts DESC rows between unbounded preceding and unbounded following) as level
FROM staging_events)
""")

song_table_insert = ("""
INSERT INTO songs
(SELECT 
    DISTINCT song_id,
    title, 
    artist_id, 
    year, 
    duration 
 FROM staging_songs)
""")

artist_table_insert = ("""
INSERT INTO artists
(SELECT 
    DISTINCT artist_id, 
    artist_name, 
    artist_location, 
    artist_latitude, 
    artist_longitude
 FROM staging_songs)
""")

time_table_insert = ("""
INSERT INTO time
(SELECT 
    DISTINCT ev.ts, 
    (TIMESTAMP 'epoch' + ev.ts/1000 * INTERVAL '1 Second ') as start_time,
    EXTRACT (hour from (TIMESTAMP 'epoch' + ev.ts/1000 * INTERVAL '1 Second ')) as hour,
    EXTRACT (day from (TIMESTAMP 'epoch' + ev.ts/1000 * INTERVAL '1 Second ')) as day,
    EXTRACT (week from (TIMESTAMP 'epoch' + ev.ts/1000 * INTERVAL '1 Second ')) as week,
    EXTRACT (month from (TIMESTAMP 'epoch' + ev.ts/1000 * INTERVAL '1 Second ')) as month,
    EXTRACT (year from (TIMESTAMP 'epoch' + ev.ts/1000 * INTERVAL '1 Second ')) as year,
    EXTRACT (dayofweek from (TIMESTAMP 'epoch' + ev.ts/1000 * INTERVAL '1 Second ')) as dayofweek
FROM staging_events ev)
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]