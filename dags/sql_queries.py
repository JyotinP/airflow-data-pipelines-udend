## dq count check

songplays_count_check = """
    SELECT COUNT(*) FROM songplays
"""

artists_count_check = """
    SELECT COUNT(*) FROM artists
"""

songs_count_check = """
    SELECT COUNT(*) FROM songs
"""

time_count_check = """
    SELECT COUNT(*) FROM time
"""

users_count_check = """
    SELECT COUNT(*) FROM users
"""


## dq duplicates check
songplays_duplicate_check = """ 
    SELECT COUNT(*) FROM 
    (SELECT songplay_id, COUNT(*) FROM songplays GROUP BY songplay_id having COUNT(*) > 1) AS duplicate_counts
"""

artists_duplicate_check = """ 
    SELECT COUNT(*) FROM 
    (SELECT artist_id, COUNT(*) FROM artists GROUP BY artist_id having COUNT(*) > 1) AS duplicate_counts
"""

songs_duplicate_check = """ 
    SELECT COUNT(*) FROM 
    (SELECT song_id, COUNT(*) FROM songs GROUP BY song_id having COUNT(*) > 1) AS duplicate_counts
"""

time_duplicate_check = """ 
    SELECT COUNT(*) FROM 
    (SELECT start_time, COUNT(*) FROM time GROUP BY start_time having COUNT(*) > 1) AS duplicate_counts
"""

users_duplicate_check = """ 
    SELECT COUNT(*) FROM 
    (SELECT user_id, COUNT(*) FROM users GROUP BY user_id having COUNT(*) > 1) AS duplicate_counts
"""


# # CREATE TABLES
staging_events_table_create= """
CREATE TABLE IF NOT EXISTS staging_events (
    artist_name   VARCHAR,
    auth          VARCHAR,
    first_name    VARCHAR,
    gender        CHAR(1),
    itemInSession SMALLINT,
    last_name     VARCHAR,
    length        DECIMAL(9,5),
    level         VARCHAR,
    location      VARCHAR,
    method        VARCHAR,
    page          VARCHAR,
    registration  BIGINT,
    session_id    INT,
    song_title    VARCHAR,
    status        SMALLINT,
    timestamp     BIGINT,
    user_agent    VARCHAR,
    user_id       INT
)
"""

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS staging_songs (
    artist_id        VARCHAR,
    artist_latitude  DECIMAL(10,8),
    artist_longitude DECIMAL(11,8),
    artist_location  VARCHAR,
    artist_name      VARCHAR,
    song_id          VARCHAR,
    title            VARCHAR,
    duration         DECIMAL(9,5),
    year             SMALLINT
)
"""

songplays_table_create = """
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id  INT IDENTITY(0, 1),
    start_time   TIMESTAMP sortkey NOT NULL,
    user_id      INT,
    level        VARCHAR,
    song_id      VARCHAR distkey NOT NULL,
    artist_id    VARCHAR,
    session_id   INT,
    location     VARCHAR,
    user_agent   VARCHAR
)
"""

users_table_create = """
CREATE TABLE IF NOT EXISTS users (
    user_id INT  NOT NULL sortkey,
    first_name   VARCHAR,
    last_name    VARCHAR,
    gender       CHAR(1),
    level        VARCHAR
) diststyle all;
"""

songs_table_create = """
CREATE TABLE IF NOT EXISTS songs (
    song_id    VARCHAR NOT NULL sortkey distkey,
    title      VARCHAR NOT NULL,
    artist_id  VARCHAR,
    year       SMALLINT,
    duration   DECIMAL(9,5)
)
"""

artists_table_create = """
CREATE TABLE IF NOT EXISTS artists (
    artist_id  VARCHAR NOT NULL sortkey, 
    name       VARCHAR NOT NULL, 
    location   VARCHAR, 
    latitude   DECIMAL(10,8), 
    longitude  DECIMAL(11,8)
) diststyle all;
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS time (
    start_time  TIMESTAMP NOT NULL sortkey, 
    hour        SMALLINT, 
    day         SMALLINT, 
    week        SMALLINT, 
    month       SMALLINT, 
    year        SMALLINT, 
    weekday     VARCHAR
) diststyle all;
"""


# FINAL TABLES
##"""Inserts data into songplays table from staging_events & staging_songs tables """
songplays_table_insert = """
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT TIMESTAMP 'epoch' + e.timestamp/1000 * INTERVAL '1 second',
           e.user_id, e.level, s.song_id, s.artist_id, e.session_id, e.location, e.user_agent
      FROM staging_events e
      JOIN staging_songs s    
        ON e.artist_name = s.artist_name
       AND e.song_title = s.title
     WHERE e.page = 'NextSong'
"""

##"""Inserts data into users table from staging_events table"""
users_table_insert = """
INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT 
           user_id, first_name, last_name, gender,
           FIRST_VALUE(level) OVER (PARTITION BY user_id, first_name, last_name, gender 
                                        ORDER BY first_name ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
      FROM staging_events
     WHERE user_id IS NOT NULL
"""

##"""Inserts data into songs table from staging_songs table"""
songs_table_insert = """
INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT song_id, title, artist_id, year, duration 
      FROM staging_songs
"""

##"""Inserts data into artists table from staging_songs table"""
artists_table_insert = """
INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id,
        first_value(artist_name)      OVER (PARTITION BY artist_id
                                                ORDER BY artist_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) f_artist_name,
        first_value(artist_location)  OVER (PARTITION BY artist_id 
                                                ORDER BY artist_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) f_artist_location,
        first_value(artist_latitude)  OVER (PARTITION BY artist_id 
                                                ORDER BY artist_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) f_artist_latitude,
        first_value(artist_longitude) OVER (PARTITION BY artist_id 
                                                ORDER BY artist_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) f_artist_longitude
      FROM staging_songs
"""

##"""Inserts data into time table from songplays table"""
time_table_insert = """
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT 
        start_time,
        extract(hour    FROM start_time),
        extract(day     FROM start_time),
        extract(week    FROM start_time),
        extract(month   FROM start_time),
        extract(year    FROM start_time),
        extract(weekday FROM start_time)
      FROM songplays
"""
