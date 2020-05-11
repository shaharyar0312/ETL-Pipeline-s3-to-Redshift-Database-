import configparser


# CONFIG VARIABLE ASSIGNMENT TO CONNECT WITH S3 DATA PATH AND IAMROLE  
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA = config.get("S3","LOG_DATA")
LOG_PATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
IAM_ROLE = config.get("IAM_ROLE","ARN")


# DROP ALL TABLES (STAGING, DIMENSIONS AND FACT)

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists fact_songplay"
user_table_drop = "drop table if exists dim_user"
song_table_drop = "drop table if exists dim_song"
artist_table_drop = "drop table if exists dim_artist"
time_table_drop = "drop table if exists dim_time"

# CREATE ALL TABLES (STAGING, DIMENSIONS AND FACT)


staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
artist          VARCHAR,
auth            VARCHAR, 
firstName       VARCHAR,
gender          VARCHAR,   
itemInSession   INTEGER,
lastName        VARCHAR,
length          FLOAT,
level           VARCHAR, 
location        VARCHAR,
method          VARCHAR,
page            VARCHAR,
registration    BIGINT,
sessionId       INTEGER,
song            VARCHAR,
status          INTEGER,
ts              TIMESTAMP,
userAgent       VARCHAR,
userId          INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
song_id            VARCHAR,
num_songs          INTEGER,
title              VARCHAR,
artist_name        VARCHAR,
artist_latitude    FLOAT,
year               INTEGER,
duration           FLOAT,
artist_id          VARCHAR,
artist_longitude   FLOAT,
artist_location    VARCHAR
);
""")

# IN THIS FACT TABLE SONGPLAY_ID SET AS SORTKEY BEACUSE FACTS USUALLY HAVE LARGE AMOUNT OF DATA AND TO REDUCE QUERY TIME 
# user_id, song_id, artist_id THESE FOREIGN KEYS ARE SET AS NOT NULL
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS fact_songplay
(
songplay_id          INTEGER IDENTITY(0,1) PRIMARY KEY sortkey,
start_time           TIMESTAMP,
user_id              INTEGER not null,
level                VARCHAR,
song_id              VARCHAR not null,
artist_id            VARCHAR not null,
session_id           INTEGER,
location             VARCHAR,
user_agent           VARCHAR
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_user
(
user_id INTEGER PRIMARY KEY distkey,
first_name      VARCHAR,
last_name       VARCHAR,
gender          VARCHAR,
level           VARCHAR
);
""")

# IN THIS FACT TABLE artist_id SET AS distkey TO HAVE SAME ARTIST KEY ON THE SAME SLICE OF CPU AND TO REDUCE QUERY TIME 
song_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_song
(
song_id     VARCHAR PRIMARY KEY,
title       VARCHAR,
artist_id   VARCHAR distkey not null,
year        INTEGER,
duration    FLOAT
);
""")

# IN THIS FACT TABLE artist_id SET AS distkey TO HAVE SAME ARTIST KEY ON THE SAME SLICE OF CPU AND TO REDUCE QUERY TIME 
artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_artist
(
artist_id          VARCHAR PRIMARY KEY distkey,
name               VARCHAR,
location           VARCHAR,
latitude           FLOAT,
longitude          FLOAT
);
""")

# start_time SET AS SORTKEY AND DISTKEY BECAUSE TIME IS USUALLY USE TO SORT 
time_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_time
(
start_time    TIMESTAMP PRIMARY KEY sortkey distkey,
hour          INTEGER,
day           INTEGER,
week          INTEGER,
month         INTEGER,
year          INTEGER,
weekday       INTEGER
);
""")

# STAGING TABLES
# TIMEFORMAT as 'epochmillisecs' USED TO SET TIMESTAMP VALUE INTO MILLISECS
# TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL -> THESE THREE PROPERTIES ARE USED TO CONVERT EMPTY AND BLANK VALUES TO NULL TO ENSURE DATA CONSISTENCY


staging_events_copy = ("""
    COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    region 'us-west-2'
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    FORMAT AS JSON {};
""").format(LOG_DATA, IAM_ROLE, LOG_PATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    region 'us-west-2'
    FORMAT AS JSON 'auto' 
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(SONG_DATA, IAM_ROLE)

# FINAL TABLES
# DISTINCT WITH COLUMN TS IS USED TO ENSURE UNIQUE VALUES BECAUSE start_time IS PRIMARY KEY
songplay_table_insert = ("""
INSERT INTO fact_songplay
SELECT DISTINCT to_timestamp(to_char(se.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS') start_time,
                se.userId as user_id,
                se.level as level,
                ss.song_id as song_id,
                ss.artist_id as artist_id,
                se.sessionId as session_id,
                se.location as location,
                se.userAgent as user_agent
FROM staging_events se
JOIN staging_songs ss ON se.song = ss.title AND se.artist = ss.artist_name;
""")

# user_id NOT NULL VALUES SELECTED TO ENSURE PRIMARY KEY IS NOT NULL

user_table_insert = ("""
insert into dim_user 
select 
userId as user_id,
firstName first_name,
lastName as last_name,
gender,
level           
from staging_events
where user_id is not null
""")

# song_id NOT NULL VALUES SELECTED TO ENSURE PRIMARY KEY IS NOT NULL
song_table_insert = ("""
INSERT INTO dim_song
SELECT DISTINCT song_id as song_id,
                title,
                artist_id,
                year,
                duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

# artist_id NOT NULL VALUES SELECTED TO ENSURE PRIMARY KEY IS NOT NULL

artist_table_insert = ("""
INSERT INTO dim_artist
SELECT DISTINCT artist_id as artist_id,
                artist_name as name,
                artist_location as location,
                artist_latitude as latitude,
                artist_longitude as longitude
FROM staging_songs
where artist_id IS NOT NULL;
""")

# ts NOT NULL VALUES SELECTED TO ENSURE PRIMARY KEY IS NOT NULL

time_table_insert = ("""
INSERT INTO dim_time
SELECT distinct ts as ts,
    EXTRACT(hour from ts) as hour,
    EXTRACT(day from ts) as day,
    EXTRACT(week from ts) as week,
    EXTRACT(month from ts) as month,
    EXTRACT(year from ts) as year,
    EXTRACT(weekday from ts) as weekday
FROM staging_events
WHERE ts IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert,artist_table_insert, song_table_insert, time_table_insert,songplay_table_insert]
