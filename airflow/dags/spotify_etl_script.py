import spotipy
import pandas as pd
from spotipy.oauth2 import SpotifyClientCredentials
import requests
import logging as log
import os 


SPOTIPY_CLIENT_ID = os.environ["SPOTIPY_CLIENT_ID"]
SPOTIPY_CLIENT_SECRET = os.environ["SPOTIPY_CLIENT_SECRET"]

### Extraction Steps ###
# Retrieving json with information about the top 10 records of the week
def get_record_json():

  session = requests.Session()
  session.verify = True

  spotify = spotipy.Spotify(client_credentials_manager = SpotifyClientCredentials(client_id = SPOTIPY_CLIENT_ID, client_secret = SPOTIPY_CLIENT_SECRET), requests_session = session)
  results = spotify.playlist_items('https://open.spotify.com/playlist/37i9dQZF1DXcBWIGoYBM5M', limit = 10)

  log.info("Completed get record function")

  return results


# Creating a df that will return the top ten records of the week
def retrieve_top_ten_records():

  results = get_record_json()
  d = []
  
  for record in results["items"]:
    d.append(
          {
            'Track Name': record['track']["name"],
            'Track URI': record['track']["uri"],
            'Artist': record['track']['album']['artists'][0]['name'],
            'Artist URI': record['track']['album']['artists'][0]['id'],
            'Release Date': record['track']['album']["release_date"]
          }
      )

  song_df = pd.DataFrame(d)
  log.info("Completed retrieve_top_ten_records function")

  return(song_df)


# Creating another df, this will return Information about the artist who held top 10 songs this week
def retrieve_info_from_top_ten_artist():

  song_df = retrieve_top_ten_records()
  artist_of_interest = song_df['Artist URI'].tolist()

  session = requests.Session()
  session.verify = True

  auth_manager = SpotifyClientCredentials(client_id = SPOTIPY_CLIENT_ID, client_secret = SPOTIPY_CLIENT_SECRET)
  spotify = spotipy.Spotify(auth_manager = auth_manager,requests_session=session)

  b = []
  for artists in artist_of_interest:
    artists_data = spotify.artist(artists)
    b.append(
          {
            'Artist ID': artists_data['id'],
            'Artist Name': artists_data['name'],
            'Artist Genre': artists_data['genres'],
            'Artist Followers': artists_data['followers']['total'],
          }
      )

  artist_df = pd.DataFrame(b)
  log.info("Completed retrieve_info_from_top_ten_artist function")
  return(artist_df)


### Tranformation Steps ###

def clean_song_df(load_data):
  load_data.insert(5, 'Extract Timestamp', pd.to_datetime('now', utc = True).replace(microsecond=0))
  load_data["Primary Key"] = load_data["Track URI"] + " " + load_data["Extract Timestamp"].dt.strftime('%Y/%m/%d %H:%M:%S')
  load_data.insert(0, 'Weekly Chart Position', range(1,11))
  log.info("Completed clean_song_df function")

  return load_data

def clean_artist_df(load_data):
  load_data.insert(4, 'Extract Timestamp', pd.to_datetime('now', utc = True).replace(microsecond=0))
  load_data["Primary Key"] = load_data["Artist ID"] + " " + load_data["Extract Timestamp"].dt.strftime('%Y/%m/%d %H:%M:%S')
  load_data['Artist Genre'] = load_data['Artist Genre'].apply(lambda x: ', '.join(map(str,x)))
  load_data['Artist Genre'] = load_data['Artist Genre'].str.replace('[', '').str.replace(']', '')
  log.info("Completed clean_artist_df function")

  return load_data


# Quality checks on the two cleaned dfs
def Data_Quality(*args):
  for load_df in args:
    
    #Ensuring the df has data no emptys
    if load_df.empty:
        print('No Songs Extracted')
        return False

    #Enforcing Primary keys since we don't need duplicates
    if pd.Series(load_df['Primary Key']).is_unique:
       pass
    else:
        #If duplicates in the PK go shut it down
        raise Exception("Primary Key contains duplicates")

    #Checking for Nulls in our PK
    if load_df['Primary Key'].isnull().values.any():
        raise Exception("Primary Key has Null Values")

  log.info("Completed Data_Quality function")
  return True


#### Load Step ####
def spotify_etl():

  song_df = retrieve_top_ten_records()
  artist_df = retrieve_info_from_top_ten_artist()

  transformed_artist_df = clean_artist_df(artist_df)    
  transformed_song_df = clean_song_df(song_df)

  Data_Quality(transformed_artist_df, transformed_song_df)

  log.info("Completed spotify_etl function")
  return transformed_artist_df, transformed_song_df

