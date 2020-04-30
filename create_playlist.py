import json
import os
import requests

import google_auth_oauthlib.flow
import googleapiclient.discovery
import googleapiclient.errors
import youtube_dl

from exceptions import ResponseException
from secrets import spotify_token, spotify_user_id


class CreatePlaylist:
    def __init__(self):
        self.youtube_client = self.get_youtube_client()
        self.all_song_info = {}

    def get_youtube_client(self):
        # Log into Youtube, copied from Youtube API
        api_service_name = 'youtube'
        api_version = 'v3'
        client_secrets_file = 'client_secret.json'

        # Get credentials and create an API client
        scopes = ['https://www.googleapis.com/auth/youtube.readonly']
        flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
            client_secrets_file, scopes
        )
        credentials = flow.run_console()

        # From the Youtube Data API
        youtube_client = googleapiclient.discovery.build(
            api_service_name, api_version, credentials=credentials
        )

        return youtube_client

    def get_liked_videos(self):
        # Fetch our Liked Videos & create a dictionary of important song info
        request = self.youtube_client.videos().list(
            part = 'snippet,contentDetails,statistics',
            myRating = 'like'
        )
        response = request.execute()

        # Collect each video and get important info
        for item in response['items']:
            video_title = item['snippet']['title']
            youtube_url = 'https://www.youtube.com/watch?v={}'.format(
                item['id'])

            # Use youtube_dl to collect the song & and artist names
            video = youtube_dl.YoutubeDL({}).extract_info(
                youtube_url, download=False)
            song_name = video['track']
            artist = video['artist']

            if song_name is not None and artist is not None:
                # Save all important info and skip any missing song and artist
                self.all_song_info[video_title] = {
                    'youtube_url': youtube_url,
                    'song_name': song_name,
                    'artist': artist,

                    # Add the uri, easy to get song to put into playlist
                    'spotify_uri': self.get_spotify_uri(song_name, artist)
                }

    def create_playlist(self):
        request_body = json.dumps({
            'name': 'Youtube Liked Vids',
            'description': 'All Liked Youtube Videos',
            'public': False
        })

        query = 'https://api.spotify.com/v1/users/{}/playlists'.format(
            spotify_user_id
        )
        response = requests.post(
            query,
            data=request_body,
            headers={
                'Content-Type': 'application/json',
                'Authorization': 'Bearer {}'.format(spotify_token)
            }
        )
        response_json = response.json()

        return response_json['id']

    def get_spotify_uri(self, song_name, artist):
        # Searching for song
        query = 'https://api.spotify.com/v1/search?q=track%3A{}+artist%3A{}&type=track&offset=0&limit=20'.format(
            song_name, artist
        )
        response = requests.get(
            query,
            headers={
                'Content-Type': 'application/json',
                'Authorization': 'Bearer {}'.format(spotify_token)
            }
        )
        response_json = response.json()
        songs = response_json['tracks']['items']

        # Only grab the first song
        uri = songs[0]['uri']

        return uri

    def add_song_to_playlist(self):
        # Add all liked songs into a new Spotify playlist
        # Populate dictionary with our liked songs
        self.get_liked_videos()

        # Collect all of uri
        uris = [info['spotify_uri']
                for song, info in self.all_song_info.items()]

        # Create a new playlist
        playlist_id = self.create_playlist()

        # Add all songs into new playlist
        request_data = json.dumps(uris)

        query = 'https://api.spotify.com/v1/playlists/{}/tracks'.format(
            playlist_id
        )

        response = requests.post(
            query,
            data=request_data,
            headers={
                'Content-Type': 'application/json',
                'Authorization': 'Bearer {}'.format(spotify_token)
            }
        )

        # Check for valid response status
        if response.status_code not in (200, 201):
            raise ResponseException(response.status_code)

        return response.json()


if __name__ == '__main__':
    cp = CreatePlaylist()
    cp.add_song_to_playlist()