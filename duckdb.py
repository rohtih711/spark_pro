import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

client_credentials_manager = SpotifyClientCredentials(
    client_id='88a9b3c2af894fc9879249c4296bb54e', 
    client_secret='3ad7280f82fe40e38d68750f0125c999' )

sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

playlist_link = 'https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF?si=1333723a6eff4b7f&nd=1&dlsi=f798f9be51c54554'

playlist_link_URI = playlist_link.split('/')[-1].split('?')[0]

data = sp.playlist_tracks(playlist_link_URI)

print(data)

