import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN = config.get("IAM_ROLE", "ARN")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist text,
        auth text,
        firstName text,
        gender char(1),
        itemInSession int,
        lastName text,
        length numeric,
        level text,
        location text,
        method text,
        page text,
        registration text,
        sessionId int,
        song text,
        status int,
        ts bigint,
        userAgent text,
        userId int        
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs int, 
        artist_id text, 
        artist_latitude text, 
        artist_longitude text, 
        artist_location text, 
        artist_name text, 
        song_id text, 
        title text, 
        duration numeric, 
        year int
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id int IDENTITY(0, 1) PRIMARY KEY,
        start_time timestamp NOT NULL sortkey, 
        user_id int NOT NULL distkey,
        level text NOT NULL, 
        song_id text, 
        artist_id text, 
        session_id int NOT NULL, 
        location text NOT NULL, 
        user_agent text NOT NULL    
    )
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id int PRIMARY KEY, 
    first_name text NOT NULL, 
    last_name text NOT NULL, 
    gender text NOT NULL, 
    level text NOT NULL
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id text PRIMARY KEY, 
    title text NOT NULL, 
    artist_id text NOT NULL distkey, 
    year int NOT NULL, 
    duration numeric NOT NULL
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id text PRIMARY KEY, 
    name text NOT NULL sortkey, 
    location text, 
    latitude text, 
    longitude text    
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time timestamp PRIMARY KEY sortkey, 
    hour int NOT NULL, 
    day int NOT NULL, 
    week int NOT NULL, 
    month int NOT NULL, 
    year int NOT NULL, 
    weekday int NOT NULL
)
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
        FROM {0}
        IAM_ROLE {1}
        JSON {2};
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs
        FROM {0}
        IAM_ROLE {1}
        JSON 'auto';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
SELECT timestamp without time zone 'epoch' + e.ts/1000*interval '1s', e.userId, e.level, s.song_id, s.artist_id, e.sessionId, e.location, e.userAgent
FROM staging_events e 
INNER JOIN staging_songs s
ON e.song = s.title AND e.artist = s.artist_name
WHERE e.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level) 
SELECT DISTINCT e.userId, e.firstName, e.lastName, e.gender, e.level
FROM staging_events e
WHERE e.page = 'NextSong' AND e.userId IS NOT NULL
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) 
SELECT DISTINCT s.song_id, s.title, s.artist_id, s.year, s.duration
FROM staging_songs s
WHERE s.song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude) 
SELECT DISTINCT s.artist_id, s.artist_name, s.artist_location, s.artist_latitude, s.artist_longitude
FROM staging_songs s
WHERE s.artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
SELECT start_time,
    EXTRACT(hour from start_time),
    EXTRACT(day from start_time),
    EXTRACT(week from start_time),
    EXTRACT(month from start_time),
    EXTRACT(year from start_time),
    EXTRACT(weekday from start_time)
FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
