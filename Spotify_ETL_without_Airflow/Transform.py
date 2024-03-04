import Extract
import pandas as pd


#function to clean the song_df
def clean_song_df(load_data):
  load_data.insert(5, 'Extract Timestamp', pd.to_datetime('now', utc = True).replace(microsecond=0))
  load_data["Primary Key"] = load_data["Track URI"] + " " + load_data["Extract Timestamp"].dt.strftime('%Y/%m/%d %H:%M:%S')
  load_data.insert(0, 'Weekly Chart Position', range(1,11))
  return load_data


#function to clean the artist_df
def clean_artist_df(load_data):
  load_data.insert(4, 'Extract Timestamp', pd.to_datetime('now', utc = True).replace(microsecond=0))
  load_data["Primary Key"] = load_data["Artist ID"] + " " + load_data["Extract Timestamp"].dt.strftime('%Y/%m/%d %H:%M:%S')
  load_data['Artist Genre'] = load_data['Artist Genre'].apply(lambda x: ', '.join(map(str,x)))
  load_data['Artist Genre'] = load_data['Artist Genre'].str.replace('[', '').str.replace(']', '')
  return load_data


#function to do some quick testing on the data
def Data_Quality(*args):
  for load_df in args:
    
    #Ensuring the df has data
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

  return True



if __name__ == "__main__":

    #Importing the songs_df from the extract.py
    song_df = Extract.retrieve_top_ten_records()
    artist_df = Extract.retrieve_info_from_top_ten_artist()

    # #calling the transformation
    transformed_artist_df = clean_artist_df(artist_df)    
    transformed_song_df = clean_song_df(song_df)

    # #running test on dfs
    Data_Quality(artist_df, song_df)