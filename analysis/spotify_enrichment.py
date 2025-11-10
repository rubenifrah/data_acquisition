import pandas as pd
import spotipy
import time
import os
from spotipy.oauth2 import SpotifyClientCredentials
from dotenv import load_dotenv
from tqdm import tqdm

# Register tqdm with pandas to use .progress_apply()
tqdm.pandas(desc="Searching for tracks")

def get_spotify_client():
    """
    Initializes and returns a Spotipy client using credentials
    from a .env file.
    """
    # ... (No changes here) ...
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

def search_spotify_track(sp, song_name, artist_name):
    """
    Searches Spotify for a single track using a 2-step cascade logic.
    Returns the track ID and popularity if found.
    
    Step 1: Try a strict query with track and artist filters.
    Step 2: If strict fails, try a broad "flexible" query.
    """
    
    # --- Attempt 1: Strict Search ---
# ... (No changes here) ...
    query_strict = f'track:"{song_name}" artist:"{artist_name}"'
    
    try:
        result_strict = sp.search(q=query_strict, limit=1, type='track')
        
        if result_strict and result_strict['tracks']['items']:
            # Found a high-confidence match!
            track = result_strict['tracks']['items'][0]
            return track['id'], track['popularity']
            
    except Exception as e:
        print(f"Error on strict search '{query_strict}': {e}")
        time.sleep(5) # Wait 5 seconds if we get rate-limited or an error

    # --- Attempt 2: Flexible Search (Fallback) ---
# ... (No changes here) ...
    query_flexible = f"{song_name} {artist_name}"
    
    try:
        result_flexible = sp.search(q=query_flexible, limit=1, type='track')

        if result_flexible and result_flexible['tracks']['items']:
            # Found a "best guess" match.
            track = result_flexible['tracks']['items'][0]
            return track['id'], track['popularity']
            
    except Exception as e:
        print(f"Error on flexible search '{query_flexible}': {e}")
        time.sleep(5) # Wait 5 seconds

    # If both searches failed, return None
    return None, None

def main():
    """
    Main function to run the data enrichment process.
    *** UPDATED WITH RESUMABLE LOGIC ***
    """
    # --- 1. Setup ---
    SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
    PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))
    
    OUR_CSV_PATH = os.path.join(PROJECT_ROOT, 'data', 'all_songs.csv')
    # --- NEW: Intermediate file path ---
    INTERMEDIATE_CSV_PATH = os.path.join(PROJECT_ROOT, 'data', 'songs_with_spotify_ids.csv')
    OUTPUT_CSV_PATH = os.path.join(PROJECT_ROOT, 'data', 'songs_with_audio_features.csv')

    sp = get_spotify_client()
    if not sp:
        return # Stop if authentication failed

    # --- 2. Load Data ---
    # --- NEW: Check if we can resume ---
    if os.path.exists(INTERMEDIATE_CSV_PATH):
        print(f"Found existing file: {INTERMEDIATE_CSV_PATH}")
        print("Loading this file and skipping Step 1 (Search).")
        df = pd.read_csv(INTERMEDIATE_CSV_PATH)
        df['year'] = pd.to_numeric(df['year'], errors='coerce').fillna(0).astype(int)

    else:
        print("--- Step 1: No intermediate file found. Starting full search. ---")
        print(f"Loading our data from: {OUR_CSV_PATH}")
        try:
            df = pd.read_csv(OUR_CSV_PATH)
        except FileNotFoundError:
            print(f"ERROR: File not found: {OUR_CSV_PATH}")
            print("Please run the Scrapy spider first.")
            return

        # Clean data
        df = df.dropna(subset=['name', 'artist'])
        df['year'] = pd.to_numeric(df['year'], errors='coerce').fillna(0).astype(int)
        
        print(f"Loaded {len(df)} songs to process.")
        
        # --- 3. Process Songs - STEP 1: Search (Loop 1) ---
        print("--- Searching for all tracks on Spotify (This will take a long time) ---")
        
        # Use .progress_apply() from tqdm for a clean progress bar
        # This creates two new columns: 'spotify_track_id' and 'popularity'
        df[['spotify_track_id', 'popularity']] = df.progress_apply(
            lambda row: search_spotify_track(sp, row['name'], row['artist']),
            axis=1,
            result_type='expand'
        )
        
        # Report search results
        total_songs = len(df)
        total_matched = df['spotify_track_id'].notna().sum()
        match_rate = (total_matched / total_songs) * 100
        print(f"Search complete: Found {total_matched} / {total_songs} ({match_rate:.2f}%) tracks.\n")
        
        # --- NEW: Save intermediate results ---
        print(f"Saving intermediate results to: {INTERMEDIATE_CSV_PATH}")
        df.to_csv(INTERMEDIATE_CSV_PATH, index=False)
        print("Save complete. Moving to audio features.")


    # --- 4. Process Songs - STEP 2: Batch Fetch Audio Features (Loop 2) ---
    print("\n--- Step 2: Fetching audio features in batches ---")
    
    # Get a clean list of unique track IDs we found
    track_ids = df[df['spotify_track_id'].notna()]['spotify_track_id'].unique().tolist()
    
    if not track_ids:
        print("No Spotify track IDs were found. Halting.")
        return

    all_features = []
    batch_size = 100 # Spotify's limit per call
    
    for i in tqdm(range(0, len(track_ids), batch_size), desc="Fetching audio features"):
        batch = track_ids[i:i + batch_size]
        try:
            features_list = sp.audio_features(tracks=batch)
            if features_list:
                # Add valid results to our master list
                all_features.extend([f for f in features_list if f])
        except Exception as e:
            print(f"Error fetching batch {i}-{i+batch_size}: {e}")
            print("This might be the 403 error. Check app 'Users and Access' in Spotify Dashboard.")
            print("Pausing for 10 seconds...")
            time.sleep(10) # Pause longer on error

    if not all_features:
        print("Error: Could not fetch any audio features. Halting.")
        print("Your intermediate file with track IDs is safe at:")
        print(INTERMEDIATE_CSV_PATH)
        return

    # --- 5. Save Results ---
    print("\nProcessing complete. Merging and saving all data...")
    
    # Convert the features list to a DataFrame
    features_df = pd.DataFrame(all_features)
    
    # Merge our original DataFrame with the new features
    # We merge on 'spotify_track_id' (from df) and 'id' (from features_df)
    df_enriched = pd.merge(
        df, 
        features_df, 
        left_on='spotify_track_id', 
        right_on='id', 
        how='left'
    )

    # --- 6. Final Report & Cleanup ---
    
    # Re-order columns to be more logical
    key_cols = [
        'year', 'place', 'name', 'artist', 'link', 
        'spotify_track_id', 'popularity'
    ]
    # Get all other columns (the audio features)
    feature_cols = [
        c for c in df_enriched.columns if c not in key_cols and c not in df.columns
    ]
    # Drop duplicate 'id' column from the merge
    if 'id' in df_enriched.columns:
        df_enriched = df_enriched.drop(columns=['id'])
        
    # Handle cases where 'id' might be in feature_cols but not columns (edge case)
    if 'id' in feature_cols:
        feature_cols.remove('id')

    df_final = df_enriched[key_cols + feature_cols]

    # Save to the final CSV
    df_final.to_csv(OUTPUT_CSV_PATH, index=False)
    
    print(f"Successfully saved final enriched data to: {OUTPUT_CSV_PATH}")
    print("\n--- Final Report ---")
    total_songs = len(df_final)
    total_matched = df_final['spotify_track_id'].notna().sum()
    match_rate = (total_matched / total_songs) * 100
    print(f"Total songs processed: {total_songs}")
    print(f"Songs successfully matched on Spotify: {total_matched}")
    print(f"Songs with audio features found: {len(all_features)}")
    print(f"New Match Rate: {match_rate:.2f}%")

if __name__ == "__main__":
    main()