import json
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO

def album_data(spotify_data):
    album_data = []
    for data in spotify_data['items']:
        album_id = data['track']['album']['id']
        album_name = data['track']['album']['name']
        release_date = data['track']['album']['release_date']
        total_tracks = data['track']['album']['total_tracks']
        album_url = data['track']['album']['external_urls']['spotify']
        album_data.append({'album_id':album_id, 'album_name':album_name, 'release_date':release_date, 'total_tracks':total_tracks, 'album_url':album_url})
    return album_data

def artist_data(spotify_data):
    artist_data = []
    for data in spotify_data['items']:
        for key, value in data.items():
            if key == 'track':
                for artist in value['artists']:
                    artist_data.append({
                    'artist_id' : artist['id'],
                    'artist_name' : artist['name'],
                    'artist_url' : artist['external_urls']['spotify']
                    })
    return artist_data

def song_data(spotify_data):
    song_data = []
    for data in spotify_data['items']:
        song_data.append({
        'song_id' : data['track']['id'],
        'song_name' : data['track']['name'],
        'song_duration' : data['track']['duration_ms'],
        'song_url' : data['track']['external_urls']['spotify'],
        'song_popularity' : data['track']['popularity'],
        'song_added' : data['track']['album']['release_date'],
        'album_id' : data['track']['album']['id']
        })
    return song_data

def lambda_handler(event, context):
    # TODO implement
    s3 = boto3.client('s3')
    Bucket = 'spotify-etl-pipeline-sonali'
    key = 'raw_data/to_be_processed/'

    spotify_data = []
    spotify_keys = []

    for file in s3.list_objects(Bucket=Bucket, Prefix=key)['Contents']:
        file_name = file['Key']
        if file_name.endswith('.json'):
            response = s3.get_object(Bucket=Bucket, Key=file_name)
            data = response['Body']
            json_data = json.loads(data.read())
            spotify_data.append(json_data)
            spotify_keys.append(file_name)

    for data in spotify_data:
        artist_list = artist_data(data)
        song_list = song_data(data)
        album_list = album_data(data)

        album_df = pd.DataFrame(album_list)
        artist_df = pd.DataFrame(artist_list)
        song_df = pd.DataFrame(song_list)

        album_df = album_df.drop_duplicates(subset=['album_id'])
        artist_df = artist_df.drop_duplicates(subset=['artist_id'])
        song_df = song_df.drop_duplicates(subset=['song_id'])

        album_df['release_date'] = pd.to_datetime(album_df['release_date'], errors='coerce')
        song_df['song_added'] = pd.to_datetime(song_df['song_added'], errors='coerce')

        song_key = 'transformed_data/song_data/song_transformed_' + str(datetime.now()) + '.csv'
        artist_key = 'transformed_data/artist_data/artist_transformed_' + str(datetime.now()) + '.csv'
        album_key = 'transformed_data/album_data/album_transformed_' + str(datetime.now()) + '.csv'

        song_buffer = StringIO()
        artist_buffer = StringIO()
        album_buffer = StringIO()

        song_df.to_csv(song_buffer, index=False)
        artist_df.to_csv(artist_buffer, index=False)
        album_df.to_csv(album_buffer, index=False)

        song_content = song_buffer.getvalue()
        artist_content = artist_buffer.getvalue()
        album_buffer = album_buffer.getvalue()

        s3.put_object(Bucket=Bucket, Key=song_key, Body=song_content)
        s3.put_object(Bucket=Bucket, Key=artist_key, Body=artist_content)
        s3.put_object(Bucket=Bucket, Key=album_key, Body=album_buffer)

    s3.resource = boto3.resource('s3')
    for key in spotify_keys:
        copy_source = {'Bucket': Bucket, 'Key': key}
        s3.resource.meta.client.copy(copy_source, Bucket, 'raw_data/processed/' + key.split("/")[-1])
        s3.resource.Object(Bucket, key).delete()


        