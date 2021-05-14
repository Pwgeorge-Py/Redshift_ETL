import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
staging_events_table_drop = "DROP TABLE IF EXISTS stagingevents;"
staging_songs_table_drop = "DROP TABLE IF EXISTS stagingsongs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
        CREATE TABLE stagingevents
            (
                artist VARCHAR(250), 
                auth VARCHAR(20),
                firstName VARCHAR(35),
                gender VARCHAR(1),
                iteminSession INT,
                lastName VARCHAR(35),
                length FLOAT4,
                level VARCHAR(4),
                location VARCHAR(250),
                method VARCHAR(10),
                page VARCHAR(20),
                registration VARCHAR(12),
                sessionId INT,
                song VARCHAR(250),
                status INT,
                ts BIGINT,
                userAgent VARCHAR(250),
                userId INT
            );
""")

staging_songs_table_create = ("""
        CREATE TABLE stagingsongs
            (
                num_songs INT, 
                artist_id VARCHAR(18),
                artist_latitude FLOAT8,
                artist_longitude FLOAT8,
                artist_location VARCHAR(250),
                artist_name VARCHAR(250),
                song_id VARCHAR(18),
                title VARCHAR(250),
                duration FLOAT4,
                year INT
            );
""")

songplay_table_create = ("""
        CREATE TABLE songplays
            (
                songplay_id INT IDENTITY(0,1) PRIMARY KEY, 
                start_time TIMESTAMP NOT NULL, 
                user_id INT NOT NULL, 
                level BOOLEAN NOT NULL, 
                song_id VARCHAR(18) NOT NULL, 
                artist_id VARCHAR(18) NOT NULL, 
                session_id INT NOT NULL, 
                location VARCHAR(250), 
                user_agent VARCHAR(250) NOT NULL
            );
""")

user_table_create = ("""
        CREATE TABLE users
            (
                user_id INT NOT NULL PRIMARY KEY, 
                first_name VARCHAR(35) NOT NULL, 
                last_name VARCHAR(35) NULL, 
                gender BOOLEAN NOT NULL,
                level BOOLEAN NOT NULL
            );
""")

song_table_create = ("""
        CREATE TABLE songs
            (
                song_id VARCHAR(18) NOT NULL PRIMARY KEY, 
                title VARCHAR(250) NOT NULL, 
                artist_id VARCHAR(18) NOT NULL, 
                year SMALLINT NOT NULL,
                duration FLOAT4 NOT NULL
            );
""")

artist_table_create = ("""
        CREATE TABLE artists
            (
                artist_id VARCHAR(18) NOT NULL PRIMARY KEY, 
                name VARCHAR(250) NOT NULL, 
                location VARCHAR(250), 
                lattitude FLOAT8,
                longitude FLOAT8
            );
""")

time_table_create = ("""
        CREATE TABLE time
            (
                start_time TIMESTAMP NOT NULL PRIMARY KEY, 
                hour SMALLINT NOT NULL, 
                day SMALLINT NOT NULL, 
                week SMALLINT NOT NULL, 
                month SMALLINT NOT NULL, 
                year SMALLINT NOT NULL, 
                weekday SMALLINT NOT NULL
            );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY stagingevents FROM {}
    CREDENTIALS 'aws_iam_role={}'
    json {} compupdate off REGION 'us-west-2';
""").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE','ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
    COPY stagingsongs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    json 'auto' compupdate off REGION 'us-west-2';
""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN'))

# FINAL TABLES
songplay_table_insert = ("""
    INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT
            TIMESTAMP 'epoch' + stag_evnt.ts/1000 * interval '1 second' as start_time,
            stag_evnt.userId AS user_id,
            CASE 
                WHEN stag_evnt.level = 'free' THEN 0
                WHEN stag_evnt.level = 'paid' THEN 1
            END AS level,
            stag_song.song_id AS song_id,
            stag_song.artist_id AS artist_id,
            stag_evnt.sessionId AS session_id,
            stag_evnt.location AS location,
            stag_evnt.userAgent AS user_agent
    FROM stagingevents AS stag_evnt
    JOIN stagingsongs AS stag_song 
        ON stag_song.artist_name = stag_evnt.artist
        AND stag_song.title = stag_evnt.song
        AND stag_song.duration = stag_evnt.length
    WHERE stag_evnt.page = 'NextSong';
""")

user_table_insert = ("""
     INSERT INTO users(user_id, first_name, last_name, gender, level)
        SELECT DISTINCT 
                userId AS user_id,
                firstName AS first_name,
                lastName AS last_name,
                CASE 
                    WHEN gender = 'F' THEN 0
                    WHEN gender = 'M' THEN 1
                END AS gender,
                CASE 
                    WHEN level = 'free' THEN 0
                    WHEN level = 'paid' THEN 1
                END AS level
        FROM stagingevents
        WHERE user_id IS NOT NULL
            AND user_id NOT IN (SELECT DISTINCT user_id FROM users);
""")

song_table_insert = ("""
     INSERT INTO songs(song_id, title, artist_id, year, duration)
        SELECT DISTINCT 
                song_id,
                title,
                artist_id,
                year,
                duration
        FROM stagingsongs
        WHERE song_id IS NOT NULL
            AND song_id NOT IN (SELECT DISTINCT song_id FROM songs);
""")

artist_table_insert = ("""
     INSERT INTO artists(artist_id, name, location, lattitude, longitude)
        SELECT DISTINCT
                artist_id,
                artist_name AS name,
                artist_location AS location,
                artist_latitude AS lattitude,
                artist_longitude AS longitude
        FROM stagingsongs
        WHERE artist_id IS NOT NULL
            AND artist_id NOT IN (SELECT DISTINCT artist_id FROM artists);
""")

time_table_insert = ("""
     INSERT INTO time(start_time, hour, day, week, month, year, weekday)
        SELECT  start_time,
                EXTRACT(hour FROM start_time) AS hour,
                EXTRACT(day FROM start_time) AS day,
                EXTRACT(week FROM start_time) AS week,
                EXTRACT(month FROM start_time) AS month,
                EXTRACT(year FROM start_time) AS year,
                EXTRACT(weekday FROM start_time) AS weekday
        FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' as start_time FROM stagingevents) a;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
