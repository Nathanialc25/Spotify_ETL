import Extract
import Transform
import sqlalchemy
import pandas as pd 
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3


#setting variables
DATABASE_LOCATION = "sqlite:///Spotify_etl.sqlite"


if __name__ == "__main__":

    #Importing the different dfs from the Extract.py
    song_df = Extract.retrieve_top_ten_records()
    artist_df = Extract.retrieve_info_from_top_ten_artist()

    #running transformations
    transformed_artist_df = Transform.clean_artist_df(artist_df)
    transformed_song_df = Transform.clean_song_df(song_df)

    #running data quality checks
    if(Transform.Data_Quality(transformed_artist_df, transformed_song_df) == False):
        raise ("Failed at Data Validation")

    #Establishing sqlite db, and putting data into Database
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('Spotify_etl.sqlite')
    cursor = conn.cursor()


    #SQL Query to Create artist of the week
    sql_query_1 = """
    CREATE TABLE IF NOT EXISTS song_of_the_week(
        chart_position INT,
        track_name VARCHAR(200),
        track_uri VARCHAR(200),
        artist VARCHAR(200),
        artist_uri VARCHAR(200),
        release_date DATE,
        etxract_ts TIMESTAMP,
        pk VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (pk)
    )
    """
    #SQL Query to Create Most Listened Artist
    sql_query_2 = """
    CREATE TABLE IF NOT EXISTS artist_table(
        artist_id VARCHAR(200),
        artist_name VARCHAR(200),
        artist_genre VARCHAR(200),
        artist_followers INT,
        etxract_ts TIMESTAMP,
        CONSTRAINT primary_key_constraint PRIMARY KEY (artist_id)
    )
    """

    cursor.execute(sql_query_1)
    cursor.execute(sql_query_2)
    print("Database successfully opened")

    #appending for the songs, because we always want to preserve the  old
    try:
        transformed_song_df.to_sql("song_of_the_week", engine, index=False, if_exists='append')
    except:
        print("Data already exists in the Song Table")
    #adding new rows, but overwriting artist who are already there
    try:
        transformed_artist_df.to_sql("artist_table", engine, index=False, if_exists='replace')
    except:
        print("Data already exists in the Artist table")

    conn.close()
    print("Close database successfully")
    



'''
Can always view here
https://inloop.github.io/sqlite-viewer/
'''