import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events
(
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR(50),
    gender CHAR,
    itemInSession INTEGER,
    lastName VARCHAR(50),
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT,
    sessionId INTEGER,
    song VARCHAR,
    status INTEGER,
    ts BIGINT,
    userAgent VARCHAR,
    userId INTEGER
);
""")

staging_songs_table_create = ("""
   CREATE TABLE IF NOT EXISTS staging_songs (    
    num_songs INTEGER,
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration DECIMAL(9),
    year INTEGER
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays  
(
        songplay_id INTEGER IDENTITY(1,1)  PRIMARY KEY,
        start_time TIMESTAMP               NOT NULL,
        user_id INTEGER                    NOT NULL,
        level VARCHAR                      NOT NULL,
        song_id VARCHAR                    NOT NULL,
        artist_id VARCHAR                  NOT NULL,
        session_id INTEGER                 NOT NULL,
        location VARCHAR                   NULL,
        user_agent VARCHAR                 NULL
)
DISTSTYLE KEY
DISTKEY (start_time)
SORTKEY (start_time);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users 
(
        user_id INTEGER PRIMARY KEY,
        first_name VARCHAR(50)           NULL,
        last_name VARCHAR(50)            NULL,
        gender CHAR(1) ENCODE BYTEDICT   NULL,
        level VARCHAR(10)                NULL
) 
DISTSTYLE ALL
SORTKEY (user_id);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs 
(
        song_id VARCHAR PRIMARY KEY,
        title   VARCHAR    NOT NULL,
        artist_id VARCHAR  NOT NULL,
        year INTEGER ENCODE BYTEDICT    NOT NULL, 
        duration FLOAT     NOT NULL
)
DISTSTYLE ALL
SORTKEY (song_id);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists 
(   
       artist_id VARCHAR PRIMARY KEY, 
       name VARCHAR NULL, 
       location VARCHAR  NULL, 
       latitude FLOAT    NULL,
       longitutde FLOAT  NULL
)
DISTSTYLE ALL
SORTKEY (artist_id);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
    start_time  TIMESTAMP PRIMARY KEY ,
    hour INTEGER      NULL,
    day INTEGER       NULL,
    week INTEGER      NULL,
    month INTEGER     NULL,
    year INTEGER ENCODE BYTEDICT   NULL,
    weekday VARCHAR(9) ENCODE BYTEDICT  NULL
)
DISTSTYLE KEY
DISTKEY (start_time)
SORTKEY (start_time);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events 
FROM {}
iam_role {}
json {};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs 
FROM {}
iam_role {}
json 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT 
        TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' as start_time,
                se.userId,
                se.level,
                ss.song_id,
                ss.artist_id,
                se.sessionId,
                se.location,
                se.userAgent
FROM staging_songs ss
 JOIN staging_events se
ON (ss.title = se.song AND se.artist = ss.artist_name)
AND se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender,level)
SELECT DISTINCT 
        se.userId,
        se.firstName,
        se.lastName,
        se.gender,
        se.level
FROM staging_events se
 WHERE se.page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT
    DISTINCT 
    ss.song_id, 
    ss.title, 
    ss.artist_id, 
    ss.year, 
    ss.duration
FROM staging_songs ss;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location,latitude, longitude)
SELECT 
       DISTINCT
       ss.artist_id,
       ss.artist_name,
       ss.artist_location,
       ss.artist_latitude,
       ss.artist_longitude
FROM staging_songs ss;

""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
       TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second' as start_time,
       EXTRACT(HOUR FROM start_time) AS hour,
       EXTRACT(DAY FROM start_time) AS day,
       EXTRACT(WEEKS FROM start_time) AS week,
       EXTRACT(MONTH FROM start_time) AS month,
       EXTRACT(YEAR FROM start_time) AS year,
       to_char(start_time, 'Day') AS weekday
FROM staging_events se
WHERE se.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
