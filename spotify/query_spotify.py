import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import os
from dotenv import load_dotenv
import pandas as pd
from datetime import date

#Load Environment Variables
load_dotenv()
cid = os.getenv('CLIENT_ID')
secret = os.getenv('CLIENT_SECRET')
years_back = int(os.getenv('YEARS_BACK'))
current_year = int(date.today().year)
min_year = current_year - years_back

#Authenticate
client_credentials_manager = SpotifyClientCredentials(client_id=cid, client_secret=secret)
sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)

#Get Artist Features From ID
def extract_artist_features(artist_id: str):
        artist = sp.artist(artist_id)
        loop_dict = {}
        loop_dict['id'] = artist['id']
        loop_dict['followers'] = artist['followers']['total']
        loop_dict['genres'] = artist['genres']
        loop_dict['name'] = artist['name']
        loop_dict['popularity'] = artist['popularity']
        return loop_dict

#Search Artists In a A Genre
def get_artists(genre: str, max_artists: int=10000) -> pd.DataFrame:
    """Get List of Artists Given a Genre

    Args:
        genre (str): Genre to search
        max_artists (int, optional): Max # of Artists to Return. Defaults to 10000.

    Returns:
        list: List of Albums IDs Of The Albums Albums
    """
    artist_list = []
    next = True
    loop_offset = 0
    while next:
        print('Current Number of Artists: ' + str(len(artist_list)))
        try:
            artists = sp.search(q='genre:' + genre, type='artist',limit=50, offset=loop_offset)['artists']
        except:
            print('Something off with request. For some reason at 1000 offset it stops working. Keep it moving I guess')
            next = False
        artist_list = artist_list + artists['items']
        if artists['next'] is not None:
            loop_offset = artists['offset'] + 50
        else:
            next = False
        if len(artist_list) > max_artists:
            artist_list = artist_list[:max_artists]
            next = False
    artist_pd = []
    for i in artist_list:
        print('Running Artist: ' + str(artist_list.index(i) + 1))
        loop_dict = {}
        loop_dict['id'] = i['id']
        loop_dict['followers'] = i['followers']['total']
        loop_dict['genres'] = i['genres']
        loop_dict['name'] = i['name']
        loop_dict['popularity'] = i['popularity']
        artist_pd.append(loop_dict)
    artist_pd = pd.DataFrame(artist_pd).drop_duplicates(subset=['id']).reset_index(drop=True)
    return artist_pd

#Query Songs In Albums For A Given Artist And Extract Edge Data We Want
def get_tracks(artist_id: str, max_albums: int=10000, max_tracks: int=10000) -> pd.DataFrame:
    #Query Albums For A Given Artist
    album_list = []
    next = True
    loop_offset = 0
    while next:
        try:
            artist_albums = sp.artist_albums(artist_id, album_type='album', limit=50, offset=loop_offset)
        except:
            print('Something off with request. For some reason at 1000 offset it stops working. Keep it moving I guess')
            next = False
        album_list = album_list + [x['id'] for x in artist_albums['items'] if int(x['release_date'].split('-')[0]) >= min_year]
        if artist_albums['next'] is not None:
            loop_offset = artist_albums['offset'] + 50
        else:
            next = False
        if len(album_list) > max_albums:
            album_list = album_list[:max_albums]
            next = False
    track_pd = []
    for i in album_list:
        print('Running Album: ' + str(album_list.index(i)+1) + ' of ' + str(len(album_list)))
        tracks_info = []
        tracks = sp.album_tracks(i, limit=50)['items']
        for t in tracks:
            track = {}
            artists = t['artists']
            artists = [(x['id'], x['name']) for x in artists]
            eligible_artists = [x[0] for x in artists]
            track['id'] = t['id']
            track['artists'] = artists
            if artist_id in eligible_artists and len(eligible_artists) > 1:
                track_details = sp.track(t['id'])
                track['popularity'] = track_details['popularity']
                track['release_date'] = track_details['album']['release_date']
                tracks_info.append(track)
        track_pd = track_pd + tracks_info
    track_pd = pd.DataFrame(track_pd).drop_duplicates(subset=['id']).reset_index(drop=True)
    return track_pd

#Get Rap Artists
artists = get_artists('rap')

#Loop Through Artists and Get Tracks
tracks = pd.DataFrame()
for index, row in artists.iterrows():
    print('Running Artist: ' + str(index+1) + ' of ' + str(len(artists)))
    artist_tracks = get_tracks(row['id'])
    tracks = pd.concat([tracks, artist_tracks]).drop_duplicates(subset=['id']).reset_index(drop=True)
    
#We Now Have New Artists That Might Not Be in Our Genre Artist Query. We Need Their Features (followers, genres, name, popularity)
track_artists = tracks.artists.values.tolist()
track_artists = list(set([item[0] for sublist in track_artists for item in sublist]))
track_artists = [x for x in track_artists if x not in artists.id.values.tolist()]
new_artists = []
for artist in track_artists:
    print('Running Album: ' + str(track_artists.index(artist)+1) + ' of ' + str(len(track_artists)))
    new_artists.append(extract_artist_features(artist))
new_artists = pd.DataFrame(new_artists).drop_duplicates(subset=['id']).reset_index(drop=True)
artists = pd.concat([artists, new_artists]).drop_duplicates(subset=['id']).reset_index(drop=True)

#We Now Have A Dataframe Called Artists and a Dataframe Called Tracks
#This is All We Need to Create Graph
