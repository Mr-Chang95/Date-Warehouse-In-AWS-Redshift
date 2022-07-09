import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA  = config.get("S3", "LOG_DATA")
LOG_PATH  = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
IAM_ROLE  = config.get("IAM_ROLE","ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= """
CREATE TABLE IF NOT EXISTS staging_events(
        artist VARCHAR(MAX),
        auth VARCHAR(MAX),
        firstName VARCHAR(MAX),
        gender VARCHAR(MAX),
        itemInSession INT,
        lastName VARCHAR(MAX),
        length FLOAT,
        level VARCHAR(MAX),
        location VARCHAR(MAX),
        method VARCHAR(MAX),
        page VARCHAR(MAX),
        registration VARCHAR(MAX),
        sessionId INT,
        song VARCHAR(MAX),
        status INT,
        ts BIGINT,
        userAgent VARCHAR(MAX),
        userId INT
)
"""

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS staging_songs(
        song_id VARCHAR(MAX) PRIMARY KEY,
        artist_id VARCHAR(MAX),
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_location VARCHAR(MAX),
        artist_name VARCHAR(MAX),
        duration FLOAT,
        num_songs INT,
        title VARCHAR(MAX),
        year INT
    )
"""

songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplays(
        songplay_id INT IDENTITY(0,1) primary key,
        start_time TIMESTAMP NOT NULL sortkey distkey,
        user_id INT NOT NULL,
        level VARCHAR(MAX),
        song_id VARCHAR(MAX) NOT NULL,
        artist_id VARCHAR(MAX) NOT NULL,
        session_id INT,
        location VARCHAR(MAX),
        user_agent VARCHAR(MAX)
    )
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS users(
        user_id VARCHAR(MAX) PRIMARY KEY NOT NULL,
        first_name VARCHAR(MAX),
        last_name VARCHAR(MAX),
        gender VARCHAR(MAX),
        level VARCHAR(MAX)
    )
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS songs(
        song_id VARCHAR(MAX) PRIMARY KEY NOT NULL,
        title VARCHAR(MAX) NOT NULL,
        artist_id VARCHAR(MAX) NOT NULL,
        year INT,
        duration FLOAT
    )
"""

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
        artist_id VARCHAR(MAX) PRIMARY KEY NOT NULL,
        name VARCHAR(MAX),
        location VARCHAR(MAX),
        latitude FLOAT,
        longitude FLOAT
    )
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
    (
        start_time  TIMESTAMP NOT NULL distkey sortkey primary key,
        hour        INT NOT NULL,
        day         INT NOT NULL,
        week        INT NOT NULL,
        month       INT NOT NULL,
        year        INT NOT NULL,
        weekday     VARCHAR(MAX) NOT NULL
    )
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events FROM {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    format as JSON {}
    timeformat as 'epochmillisecs'
""").format(LOG_DATA, IAM_ROLE, LOG_PATH)

staging_songs_copy = ("""
copy staging_songs FROM {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    format as JSON 'auto'
""").format(SONG_DATA, IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT TIMESTAMP 'epoch' + (e.ts / 1000) * INTERVAL '1 second' as start_time,
        e.userId as user_id,
        e.level as level,
        s.song_id as song_id,
        s.artist_id as artist_id,
        e.sessionId as session_id,
        e.location as location,
        e.userAgent as user_agent
    FROM staging_events e
    JOIN staging_songs  s
    ON e.song = s.title and e.artist = s.artist_name and e.page = 'NextSong' and e.length = s.duration
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
    select
        distinct(userId) as user_id,
        firstName as first_name,
        lastName as last_name,
        gender,
        level
    FROM staging_events
    WHERE user_id IS NOT NULL
    AND page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs SELECT DISTINCT (song_id)
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists SELECT DISTINCT (artist_id)
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time
        WITH temp_time AS (SELECT TIMESTAMP 'epoch' + (ts/1000 * INTERVAL '1 second') as ts FROM staging_events)
        SELECT DISTINCT
        ts,
        extract(hour FROM ts),
        extract(day FROM ts),
        extract(week FROM ts),
        extract(month FROM ts),
        extract(year FROM ts),
        extract(weekday FROM ts)
        FROM temp_time
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
