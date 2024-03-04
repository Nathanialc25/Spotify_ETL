#imports
import spotipy
import pandas as pd
import requests
from datetime import datetime, date
import datetime
import pprint
from spotipy.oauth2 import SpotifyClientCredentials



#Setting keys for the oauth connection, need this to be hidden
SPOTIPY_CLIENT_ID ='ad476904b1cf48d48c27036066f77f50'
SPOTIPY_CLIENT_SECRET ='d9fc97dc58a3409e8fa62a2c752901de'
SPOTIPY_REDIRECT_URL= 'http://localhost'

# setting some local varibles
today = date.today()
yesterday = today - datetime.timedelta(days=1)
yesterday_unix_time = int(datetime.datetime(yesterday.year, yesterday.month, yesterday.day).timestamp())





# Using secrets to access web API for my App
spotify = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials(client_id =SPOTIPY_CLIENT_ID, client_secret=SPOTIPY_CLIENT_SECRET) )
results = spotify.playlist_items('https://open.spotify.com/playlist/37i9dQZF1DXcBWIGoYBM5M', limit= 10)



#function to return top 10 records
def retrieve_top_ten_records():
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
  return(song_df)



#Function to retrieve data from the artist
def retrieve_info_from_top_ten_artist():

  #extracting a list tof the top 10 user ids
  song_df = retrieve_top_ten_records()
  artist_of_interest =song_df['Artist URI'].tolist()

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
  return(artist_df)