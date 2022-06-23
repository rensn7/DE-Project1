import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Process a song file.
    
    Insert informations about artist in artists table and information about song in songs table
    
    Parameters
    cur : psycopg2.cursor
        the base cursor
    filepath : str
        the path of song file
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df.loc[0, ['song_id', 'title', 'artist_id', 'year', 'duration']].values.tolist()
    song_data[3] = int(song_data[3])
    song_data[4] = float(song_data[4])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df.loc[0, ['artist_id', 'artist_name', 'artist_location', 'artist_latitude',
                         'artist_longitude']].values.tolist()
    artist_data[3] = float(artist_data[3])
    artist_data[4] = float(artist_data[4])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Process a log file.
    
    Insert informations about time in time table, information about user in users table
    and a fact register in the fact table songplays
    
    Parameters
    cur : psycopg2.cursor
        the base cursor
    filepath : str
        the path of log file
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime
    df.loc[:, ['ts']] = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = (df['ts'], df['ts'].dt.hour, df['ts'].dt.day, df['ts'].dt.isocalendar().week,
                 df['ts'].dt.month, df['ts'].dt.year, df['ts'].dt.weekday)
    column_labels = ('timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    d = dict([(a, b) for a, b in zip(column_labels, time_data)])
    time_df = pd.DataFrame(d)

    for i, row in time_df.iterrows():
        row = list(row)
        for k in range(1, len(row)):
            row[k] = int(row[k])
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        row = list(row)
        row[0] = int(row[0])
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row['ts'], int(row['userId']), row['level'],
                         songid, artistid, int(row['sessionId']), row['location'],
                         row['userAgent']
                        )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Process all JSON files from a directory.
    
    Parameters
    cur : psycopg2.cursor
        the base cursor
    conn : psycopg2.connection
        the base connection
    filepath: str
        the directory path
    func: function
        the function used for process and insert the data. It should be 'process_song_file' or
        'process_log_file'
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    The main method. Creates a database connection, process and insert all log and song files into
    the database.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()