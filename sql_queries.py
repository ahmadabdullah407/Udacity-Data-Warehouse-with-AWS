import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA = config.get("S3","LOG_DATA")
SONG_DATA = config.get("S3","SONG_DATA")
LOG_JSONPATH = config.get("S3","LOG_JSONPATH")
ARN = config.get("IAM_ROLE","ARN")
# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE staging_events (
                                        event_id         BIGINT       IDENTITY(0,1),
                                        artist           TEXT,
                                        auth             TEXT,
                                        first_name       TEXT,
                                        gender           TEXT,
                                        item_in_session  INTEGER,
                                        last_name        TEXT,
                                        length           FLOAT4,
                                        level            TEXT,
                                        location         TEXT,
                                        method           TEXT,
                                        page             TEXT,
                                        registration     FLOAT8,
                                        session_id       INTEGER,
                                        song             TEXT,
                                        status           INTEGER,
                                        ts               BIGINT,
                                        user_agent       TEXT,
                                        user_id          TEXT)
                                         
                                        
""")

staging_songs_table_create = (""" CREATE TABLE staging_songs (
                                        num_songs        INTEGER,
                                        artist_id        TEXT,
                                        artist_latitude  REAL,
                                        artist_longitude REAL,
                                        artist_location  TEXT,
                                        artist_name      TEXT,
                                        song_id          TEXT,
                                        title            TEXT,
                                        duration         FLOAT4,
                                        year             INTEGER)
""")

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplays (
                                    songplay_id         BIGINT       IDENTITY(0,1) PRIMARY KEY,
                                    start_time          TIMESTAMP    NOT NULL      SORTKEY,
                                    user_id             TEXT         NOT NULL      DISTKEY,
                                    level               TEXT,
                                    song_id             TEXT,
                                    artist_id           TEXT,
                                    session_id          INTEGER,
                                    location            TEXT,
                                    user_agent          TEXT
                                    ) diststyle key;
""")

user_table_create = (""" CREATE TABLE IF NOT EXISTS users (
                                    user_id             TEXT         PRIMARY KEY   SORTKEY,
                                    first_name          TEXT         NOT NULL,
                                    last_name           TEXT         NOT NULL,
                                    gender              TEXT         NOT NULL,
                                    level               TEXT         NOT NULL
                                    ) diststyle all;
""")

song_table_create = (""" CREATE TABLE IF NOT EXISTS songs (
                                    song_id             TEXT         PRIMARY KEY   SORTKEY,
                                    title               TEXT,
                                    artist_id           TEXT,
                                    year                INTEGER,
                                    duration            FLOAT4
                                    ) diststyle all;
""")

artist_table_create = (""" CREATE TABLE IF NOT EXISTS artists (
                                    artist_id           TEXT         PRIMARY KEY   SORTKEY,
                                    name                TEXT,
                                    location            TEXT,
                                    latitude            FLOAT4,
                                    longitude           FLOAT4
                                    ) diststyle all;
""")

time_table_create = (""" CREATE TABLE IF NOT EXISTS time (
                                    start_time          TIMESTAMP    PRIMARY KEY    SORTKEY,
                                    hour                INTEGER      NOT NULL,
                                    day                 INTEGER      NOT NULL,
                                    week                INTEGER      NOT NULL,
                                    month               INTEGER      NOT NULL,
                                    year                INTEGER      NOT NULL       DISTKEY,
                                    weekday             INTEGER      NOT NULL
                                    ) diststyle key;
""")

# STAGING TABLES

staging_events_copy = ("""  COPY staging_events
                            FROM {}
                            iam_role {}
                            json {}
                            region 'us-west-2';
""").format(LOG_DATA,ARN,LOG_JSONPATH)

staging_songs_copy = ("""   COPY staging_songs
                            FROM {}
                            iam_role {}
                            json 'auto'
                            region 'us-west-2';
""").format(SONG_DATA,ARN)

# FINAL TABLES

songplay_table_insert = (""" INSERT INTO songplays (start_time, user_id,level,song_id,artist_id,session_id,location,user_agent)
                                SELECT 
                                TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second' as start_time,
                                e.user_id,e.level,s.song_id,s.artist_id,e.session_id,e.location,e.user_agent
                                FROM staging_events e LEFT JOIN staging_songs s
                                ON (e.song = s.title)
                                AND (e.artist = s.artist_name)
                                AND (e.length = s.duration)
                                WHERE e.page = 'NextSong'
""")

user_table_insert = (""" INSERT INTO users (user_id,first_name,last_name,gender,level)
                            SELECT DISTINCT e.user_id,e.first_name,e.last_name,e.gender,e.level
                            FROM staging_events e
                            WHERE e.page = 'NextSong'
""")

song_table_insert = (""" INSERT INTO songs (song_id,title,artist_id,year,duration)
                            SELECT DISTINCT song_id,title,artist_id,year,duration
                            FROM staging_songs 
                            WHERE song_id IS NOT NULL
""")

artist_table_insert = (""" INSERT INTO artists (artist_id,name,location,latitude,longitude)
                            SELECT DISTINCT artist_id,artist_name,artist_location,artist_latitude,artist_longitude
                            FROM staging_songs
                            WHERE artist_id IS NOT NULL
""")

time_table_insert = (""" INSERT INTO time (start_time,hour,day,week,month,year,weekday)
                            SELECT DISTINCT start_time,extract(hour from start_time),extract(day from start_time),
                            extract(week from start_time),extract(month from start_time),extract(year from start_time),
                            extract(weekday from start_time)
                            FROM songplays
                            WHERE start_time IS NOT NULL
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
