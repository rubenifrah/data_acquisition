import pandas as pd
import spotipy
import time
import os
from spotipy.oauth2 import SpotifyClientCredentials
from dotenv import load_dotenv
from tqdm import tqdm

def get_spotify_client():
    """
    Initializes and returns a Spotipy client using credentials
    from a .env file.
    """
    # Load environment variables from .env file
    load_dotenv()
    
    client_id = os.getenv("SPOTIPY_CLIENT_ID")
    client_secret = os.getenv("SPOTIPY_CLIENT_SECRET")
    
    if not client_id or not client_secret:
        print("ERROR: SPOTIPY_CLIENT_ID or SPOTIPY_CLIENT_SECRET not found.")
        print("Please create a .env file in the root directory with your credentials.")
        return None
        
    try:
        # Authenticate using the Client Credentials Flow
        auth_manager = SpotifyClientCredentials(client_id=client_id,
                                                client_secret=client_secret)
        sp = spotipy.Spotify(auth_manager=auth_manager)
        sp.search(q='test', limit=1) # Make a test call
        print("Successfully authenticated with Spotify.")
        return sp
    except Exception as e:
        print(f"Error authenticating with Spotify: {e}")
        return None

sp = get_spotify_client()

def search_spotify_track(sp, song_name, artist_name, year):
    """
    Searches Spotify for a single track.
    Returns the track ID and popularity if found.
    """
    # Construct a query using Spotify's advanced search syntax
    # This is much more accurate than just dumping the name
    query = f"track:\"{song_name}\" artist:\"{artist_name}\""
    
    try:
        result = sp.search(q=query, limit=1, type='track')
        
        if result and result['tracks']['items']:
            track = result['tracks']['items'][0]
            # Found a match!
            return track['id'], track['popularity']
            
        # If year-specific search fails, try a broader search
        query_simple = f"track:\"{song_name}\" artist:\"{artist_name}\""
        result_simple = sp.search(q=query_simple, limit=1, type='track')
        
        if result_simple and result_simple['tracks']['items']:
            track = result_simple['tracks']['items'][0]
            # Found a match (less accurate, but still good)
            return track['id'], track['popularity']
            
        # No match found
        return None, None
        
    except Exception as e:
        print(f"Error searching for '{query}': {e}")
        time.sleep(5) # Wait 5 seconds if we get rate-limited or an error
        return None, None
result = sp.search(q = "track:\"Beat it\" artist: \"Michael Jackson\"")
print(result)