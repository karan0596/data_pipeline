class SqlQueries:
    songplay_table_insert = ("""
        INSERT INTO songplays
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        INSERT INTO users
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        INSERT INTO songs
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        INSERT INTO artists
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        INSERT INTO time
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)

    create_songplays = ("""
        CREATE TABLE IF NOT EXISTS songplays (
            songplay_id     VARCHAR,
            start_time      TIMESTAMP,
            user_id         INTEGER,
            level           VARCHAR,
            song_id         VARCHAR,
            artist_id       VARCHAR,
            session_id      INTEGER,
            location        VARCHAR,
            useragent       VARCHAR 
        )
    """)

    create_users_table = ("""
        CREATE TABLE IF NOT EXISTS users (
            user_id       INTEGER,
            first_name    VARCHAR,
            last_name     VARCHAR,
            gender        VARCHAR,
            level         VARCHAR
        )
    """)

    create_songs_table = ("""
        CREATE TABLE IF NOT EXISTS songs (
            song_id    VARCHAR,
            title      VARCHAR,
            artist_id  VARCHAR,
            year       INTEGER,
            duration   FLOAT
        )
    """)

    create_artists_table = ("""
        CREATE TABLE IF NOT EXISTS artists (
            artist_id           VARCHAR,
            artist_name         VARCHAR,
            artist_location     VARCHAR,
            artist_latitude     FLOAT,
            artist_longitude    FLOAT
        )
    """)

    create_time_table = ("""
        CREATE TABLE IF NOT EXISTS time (
            start_time TIMESTAMP,
            hour       INTEGER,
            day        INTEGER,
            week       INTEGER,
            month      INTEGER,
            year       INTEGER,
            weekday    INTEGER
        )
    """)

    create_staging_events = ("""
        CREATE TABLE IF NOT EXISTS staging_events (
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
            ts              BIGINT,
            userAgent       VARCHAR,
            userId          INTEGER
        )
    """)

    create_staging_songs = ("""
        CREATE TABLE IF NOT EXISTS staging_songs (
            num_songs        INTEGER,
            artist_id        VARCHAR(255),
            artist_latitude  NUMERIC,
            artist_longitude NUMERIC,
            artist_location  VARCHAR(500),
            artist_name      VARCHAR(500),
            song_id          VARCHAR(255),
            title            VARCHAR(500),
            duration         NUMERIC,
            year             INTEGER
        )
    """)