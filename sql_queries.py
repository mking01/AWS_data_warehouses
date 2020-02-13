import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_events_table_create= ("""CREATE TABLE staging_events (
                                    artist varchar,
                                    auth varchar,
                                    first_name varchar,
                                    gender varchar,
                                    itemInSession int,
                                    last_name varchar,
                                    length float,
                                    level varchar,
                                    location varchar,
                                    method varchar,
                                    page varchar,
                                    registration float,
                                    sessionId int,
                                    song varchar,
                                    status int,
                                    ts timestamp,
                                    userAgent varchar,
                                    userId int NOT NULL)
                                """)

staging_songs_table_create = ("""CREATE TABLE staging_songs (
                                    num_songs int,
                                    artist_id varchar NOT NULL,
                                    latitude float,
                                    longitude float,
                                    location varchar,
                                    artist_name varchar,
                                    song_id varchar,
                                    title varchar,
                                    duration float,
                                    year int)
                                """)

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay 
                                (songplay_id int IDENTITY(0, 1) PRIMARY KEY, 
                                start_time date REFERENCES time(start_time), 
                                user_id int NOT NULL REFERENCES users(user_id), 
                                level varchar, 
                                song_id varchar REFERENCES songs(song_id), 
                                artist_id varchar NOT NULL REFERENCES artists(artist_id), 
                                session_id int, 
                                location varchar, 
                                user_agent varchar)
                            """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS users 
                            (user_id int NOT NULL SORTKEY PRIMARY KEY, 
                            first_name varchar NOT NULL, 
                            last_name varchar NOT NULL, 
                            gender varchar, 
                            level varchar)
                    """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs 
                            (song_id varchar  NOT NULL SORTKEY PRIMARY KEY, 
                            title varchar, 
                            artist_id varchar NOT NULL REFERENCES artists(artist_id), 
                            year int, 
                            duration float)
                        """)

artist_table_create = ("""CREATE TABLE artists 
                            (artist_id varchar NOT NULL SORTKEY PRIMARY KEY, 
                            name varchar, 
                            location varchar, 
                            latitude float, 
                            longitude float)
                        """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS time 
                            (start_time timestamp NOT NULL DISTKEY SORTKEY PRIMARY KEY, 
                            hour int, 
                            day int, 
                            week int, 
                            month int, 
                            year int, 
                            weekday varchar)
                        """)


# STAGING TABLES
staging_events_copy = ("""copy staging_events from {data_bucket}
                        credentials 'aws_iam_role={role_arn}'
                        region 'us-west-2' format as JSON {log_json_path}
                        timeformat as 'epochmillisecs';
                        """) \
                        .format(data_bucket = config['S3']['LOG_DATA'], \
                                role_arn = config['IAM_ROLE']['ARN'], \
                                log_json_path = config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""copy staging_songs from {data_bucket}
                        credentials 'aws_iam_role={role_arn}'
                        region 'us-west-2' format as JSON {log_json_path}
                        timeformat as 'epochmillisecs';
                        """) \
                        .format(data_bucket = config['S3']['SONG_DATA'], \
                                role_arn = config['IAM_ROLE']['ARN'], \
                                log_json_path = config['S3']['LOG_JSONPATH'])

# FINAL TABLES
# omitting songplay_id from songplay insert statement because using IDENTITY()
songplay_table_insert = ("""INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                            SELECT DISTINCT (E.ts) as start_time,
                            E.userId as user_id,
                            E.level,
                            E.song_id,
                            S.artist_id,
                            E.sessionId as session_id,
                            E.location,
                            E.userAgent as user_agent
                            FROM staging_events as E
                            JOIN staging_songs as S on (E.song_title = S.song_title) and (E.artist_name = S.artist_name)
                            WHERE E.page == 'NextSong';
                        """)

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
                        SELECT DISTINCT userId AS user_id,
                        first_name,
                        last_name,
                        gender,
                        level
                        FROM staging_events
                        WHERE userId IS NOT NULL
                        AND page == 'NextSong';
                    """)

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
                        SELECT DISTINCT song_id,
                        title,
                        artist_id,
                        year,
                        duration
                        FROM staging_songs
                        WHERE song_id IS NOT NULL;
                    """)

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
                            SELECT DISTINCT (artist_id)
                            artist_name as name,
                            location,
                            latitude,
                            longitude
                            FROM staging_songs
                            WHERE artist_id IS NOT NULL;
                        """)


time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                        SELECT DISTINCT (start_time),
                        EXTRACT(hour FROM start_time) AS hour,
                        EXTRACT(day FROM start_time) AS day,
                        EXTRACT(week FROM start_time) AS week,
                        EXTRACT(month FROM start_time) AS month,
                        EXTRACT(year FROM start_time) AS year,
                        EXTRACT(dayofweek FROM start_time) AS weekday
                        FROM songplays
                    """)

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
