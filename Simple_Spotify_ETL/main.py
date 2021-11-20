import sqlalchemy
import pandas as pd 
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3

# Choosen the location for the private DB location
DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"
# Add your Spotifx ID which you can checkout in your profile
USER_ID = ""
# Token you have to generate by the Spotify-API
TOKEN = "BQC9ISnSQxlRqNUG-6vCmEDKobTDjXveT9WEgWJFGUe08RwXWRiFnmPtL7bNsc3FuuvJYP2wtqJ4bWwYvL4pxow5ztkRlbxebK0mwUC3PY786I_P9uEjZgk2ju3VxrP0IS861BtF_0x7K7fcuU2N"

# Function who check if it's valid
def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if the DataFrame is empty
    if df.empty:
        print("No songs downloaded. Finishing execution")
        return False

    # Check the primary key fpr the Database
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated")

    # Check if theirs nulls
    if  df.isnull().values.any():
        raise Exception("Null valued found")

    # Checks that only the data from the last day get downloaded. Checks the timestamps from yesterday's date
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0,second=0, microsecond=0)

    timestamps = df["timestamp"].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp,"%Y-%m-%d") != yesterday:
            raise Exception("At least one of the returned songs does not come from within the last 24hours")


if __name__ == "__main__":
    
    # The Extract part of the ETL process
    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=TOKEN)
    }

    # Convert the timestamp to a unix timestamp
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    # Download all songs "after yesterday" (last 24 hours)
    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), headers = headers)

    data = r.json()

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    # Extract only the relevant data parts / bits of data from the json object
    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

    # Prepare a dictionary in order to turn it into a pandas dataframe below
    song_dict = {
        "song_name" : song_names,
        "artist_name": artist_names,
        "played_at" : played_at_list,
        "timestamp" : timestamps
    }

    song_df = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at", "timestamp"])

    # Validate
    if check_if_valid_data(song_df):
        print("Data valid, proceed to Load stage")

    # Load
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('my_played_tracks.sqlite')
    cursor = conn.cursor()

    # Create the databse with the columns
    sql_query = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """

    cursor.execute(sql_query)
    print("Opened database successfully")

    try:
        song_df.to_sql("my played tracks", engine, index=False, if_exists='append')
    except:
        print("Data already exists in the database")

    conn.close()
    print("Close Database successfully")


