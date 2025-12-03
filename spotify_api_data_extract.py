import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

def lambda_handler(event, context):
    # TODO implement
    client_id = os.environ['client_id']
    client_secret = os.environ['client_secret']
    client_credentials_manager = SpotifyClientCredentials(client_id,client_secret)
    
    sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)

    playlist_link = "https://open.spotify.com/playlist/3bDJLJzvUBxBV4C7mezz6p?si=mk__rgHYRYCayf1uvl8TMA"
    playlist_uri = playlist_link.split("/")[-1].split("?")[0]

    data = sp.playlist_tracks(playlist_uri)

    client = boto3.client('s3')

    filename = "spotify_raw_data_" + str(datetime.now()) + ".json"

    client.put_object(
        Bucket='spotify-etl-pipeline-sonali',
        Key='raw_data/to_be_processed/' + filename,
        Body=json.dumps(data)
    )

    
