import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs "
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
 CREATE TABLE IF NOT EXISTS staging_events (    
                              artist        VARCHAR,
                              auth          VARCHAR,
                              firstName     VARCHAR,
                              gender        VARCHAR,
                              itemInSession INTEGER,
                              lastName      VARCHAR,
                              legnth        FLOAT,
                              level         VARCHAR,
                              location      VARCHAR,
                              method        VARCHAR,
                              page          VARCHAR,
                              registration  FLOAT,    
                              sessionId     INTEGER,
                              song          VARCHAR,
                              status        INTEGER,
                              ts            BIGINT,
                              userAgent     VARCHAR,
                              userId        INTEGER
                              )
                           """)

staging_songs_table_create = ("""
 CREATE TABLE IF NOT EXISTS staging_songs (    
                              num_songs        INTEGER,
                              artist_id        VARCHAR,
                              artist_latitude  FLOAT,
                              artist_longitude FLOAT,
                              artist_location  VARCHAR,
                              artist_name      VARCHAR,
                              song_id          VARCHAR,
                              title            VARCHAR,
                              duration         FLOAT,    
                              year             INTEGER
                              )
                           """)

songplay_table_create = ("""
 CREATE TABLE IF NOT EXISTS songplays (    
        		        songplay_id         INTEGER   IDENTITY(0,1)   PRIMARY KEY,
        			start_time          TIMESTAMP NOT NULL,
        			user_id             INTEGER   NOT NULL,
      				level               VARCHAR,
       				song_id             VARCHAR,
        			artist_id           VARCHAR,
        			session_id          INTEGER,
       				location            VARCHAR,
       				user_agent          VARCHAR
                              )
                           """)


user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    				user_id    	INTEGER NOT NULL PRIMARY KEY, 
    				first_name 	VARCHAR NOT NULL, 
    				last_name 	VARCHAR, 
    				gender 		VARCHAR, 
    				level 		VARCHAR
				);
		   	     """)

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    				song_id 	VARCHAR   NOT NULL PRIMARY KEY, 
    				song_title 	VARCHAR, 
    				artist_id 	VARCHAR   NOT NULL, 
    				year 		INTEGER   NOT NULL, 
    				duration 	FLOAT
				)
			     """)



artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
        			artist_id       VARCHAR    NOT NULL PRIMARY KEY,
        			name            VARCHAR    NOT NULL,
        			location        VARCHAR,
        			latitude        FLOAT,
        			longitude       FLOAT
				  )
			       """)

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
        			start_time    TIMESTAMP       NOT NULL PRIMARY KEY,
        			hour          SMALLINT,
       				day           SMALLINT,
        			week          SMALLINT,
        			month         SMALLINT,
        			year          SMALLINT,
        			weekday       SMALLINT,
				)
			     """)

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    iam_role {}
    region 'us-west-2'
    json {};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])


staging_songs_copy = ("""
    copy staging_songs from {}
    iam_role {}
    region 'us-west-2' 
    json 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'],config['S3']['LOG_JSONPATH'])



# FINAL TABLES

songplay_table_insert = ("""
INSERT  INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT  DISTINCT   (D.ts)    AS start_time, 
		D.userId     AS user_id, 
		D.level      AS level, 
		D.song_id    AS song_id, 
		D.artist_id  AS artist_id, 
		D.sessionId  AS session_id, 
		D.location   AS location,
		D.userAgent  AS user agent
FROM staging_events as D 
JOIN staging_songs as B ON D.artist = B.artist_name
where A.page = 'NextSong';
""")

			


user_table_insert = ("""
INSERT  INTO users (user_id,first_name,last_name,gender,level)
SELECT  DISTINCT A.userId          AS user_id,
                 A.firstName       AS first_name,
           	 A.lastName        AS last_name,
            	 A.gender          AS gender,
            	 A.level           AS level
FROM staging_events AS A
WHERE A.page = 'NextSong';
""")


song_table_insert = ("""
INSERT  INTO songs (song_id, title, artist_id, year, duration)
SELECT  DISTINCT A.song_id   AS song_id,
            	 A.title     AS title,
            	 A.artistId  AS artist_id,
            	 A.year      AS year,
           	 A.duration  AS duration
FROM staging_songs AS A
WHERE song_id IS NOT NULL;
""")



artist_table_insert = ("""
INSERT INTO artists (artist_id,name,location,latitude,longitude)
SELECT  DISTINCT 	A.artist_id        AS artist_id,
            		A.artist_name      AS name,
           		A.artist_location  AS location,
            		A.artist_latitude  AS latitude,
            		A.artist_longitude AS longitude
FROM staging_songs AS A;
""")



time_table_insert = ("""
INSERT INTO    time ( start_time,hour,day,week,month,year,weekday)
SELECT  DISTINCT ( D.ts )                    	    AS start_time,
            EXTRACT(hour  	FROM start_time)    AS hour,
            EXTRACT(day   	FROM start_time)    AS day,
            EXTRACT(week  	FROM start_time)    AS week,
            EXTRACT(month 	FROM start_time)    AS month,
            EXTRACT(year  	FROM start_time)    AS year,
            EXTRACT((dayofweek  FROM start_time)    AS weekday
FROM    staging_events AS D
WHERE D.page = 'NextSong';
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
