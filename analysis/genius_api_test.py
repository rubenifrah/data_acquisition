import pandas as pd
import lyricsgenius
import time
import os
import re
from dotenv import load_dotenv
from tqdm import tqdm
import json

# --- SETTINGS ---
# Set to a small number (e.g., 10) to test.
# Set to None to run the full script.
BATCH_SIZE = None
# ------------------

# Disable unnecessary warnings from the lyricsgenius library
import logging
logging.getLogger("lyricsgenius").setLevel(logging.ERROR)

def get_genius_client():
    """
    Initializes and returns a Genius client using the token
    stored in the .env file.
    """
    load_dotenv()
    genius_token = os.getenv("GENIUS_ACCESS_TOKEN")
    
    if not genius_token:
        print("ERROR: GENIUS_ACCESS_TOKEN not found in .env file.")
        print("Please create an account at genius.com/api-clients and get a token.")
        return None
        
    try:
        # Initialize the client
        genius = lyricsgenius.Genius(genius_token,
                                     timeout=30, # Increased timeout for slow Genius API
                                     verbose=False, 
                                     remove_section_headers=True,
                                     skip_non_songs=True,
                                     sleep_time=0.5) # Be polite to the API
        print("Genius client successfully authenticated.")
        return genius
    except Exception as e:
        print(f"Error authenticating Genius: {e}")
        return None

def clean_lyrics(lyrics_text):
    """
    Cleans the raw lyrics text retrieved from Genius.
    """
    if not isinstance(lyrics_text, str):
        return None
    
    # 1. Remove the initial "EmbedShare..." string Genius sometimes adds
    lyrics_text = re.sub(r'^\d*EmbedShare URLCopyEmbedCopy', '', lyrics_text.strip())
    # 2. Remove the "... Contributors" string at the end
    lyrics_text = re.sub(r'\d+ Contributors.*$', '', lyrics_text.strip())
    # 3. Replace multiple newlines with a single one
    lyrics_text = re.sub(r'\n+', '\n', lyrics_text).strip()
    
    if not lyrics_text:
        return None
    return lyrics_text

def get_genius_data(row, genius_client):
    """
    Function to apply to each row of the DataFrame.
    Fetches all data from Genius (lyrics, pageviews, etc.).
    
    Returns a dictionary with the new data.
    """
    
    # --- Default values to return on failure ---
    default_response = {
        'lyrics': None,
        'genius_status': "Unknown Error",
        'genius_pageviews': None,
        'genius_release_date': None
    }

    song_name = row['name']
    artist_name = row['artist']
    
    try:
        # Search for the song
        song = genius_client.search_song(song_name, artist_name)
        
        if song:
            # --- We found a match! ---
            lyrics = clean_lyrics(song.lyrics)
            
            status = "Success"
            if not lyrics:
                status = "Lyrics Empty" # Found song, but no lyrics text
            
            return {
                'lyrics': lyrics,
                'genius_status': status,
                'genius_pageviews': song.pageviews if hasattr(song, 'pageviews') else None,
                'genius_release_date': song.release_date if hasattr(song, 'release_date') else None
            }
        else:
            # --- No song match found ---
            return {
                'lyrics': None,
                'genius_status': "Song Not Found",
                'genius_pageviews': None,
                'genius_release_date': None
            }
            
    except Exception as e:
        # --- An API error occurred (e.g., timeout) ---
        if "timeout" in str(e).lower():
            print(f"Timeout searching for '{song_name}'. Marking to retry.")
            # We return N/A for status so the script knows to retry this row
            default_response['genius_status'] = pd.NA
        else:
            print(f"Error searching for '{song_name}': {e}")
            default_response['genius_status'] = f"Search Error: {e}"
        
        return default_response

def main():
    """
    Main script to enrich the JSON database with lyrics from Genius.
    """
    # --- 1. Path Configuration ---
    SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
    PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))
    
    # This is now our central database file
    JSON_PATH = os.path.join(PROJECT_ROOT, 'data', 'songs_database.json')

    # --- 2. Initialize Client ---
    genius = get_genius_client()
    if not genius:
        return # Stop if auth failed

    # --- 3. Load Data (Resumable Logic) ---
    try:
        print(f"Loading database from: {JSON_PATH}...")
        with open(JSON_PATH, 'r', encoding='utf-8') as f:
            all_songs_data = json.load(f)
        
        # Convert list of dicts to DataFrame
        df = pd.DataFrame(all_songs_data)
        
        print(f"Loaded {len(df)} songs.")
        
        # Initialize new columns if they don't exist
        if 'genius_status' not in df.columns:
            print("Initializing 'genius_status' column...")
            df['genius_status'] = pd.NA
        if 'lyrics' not in df.columns:
            df['lyrics'] = pd.NA
        if 'genius_pageviews' not in df.columns:
            df['genius_pageviews'] = pd.NA
        if 'genius_release_date' not in df.columns:
            df['genius_release_date'] = pd.NA
            
    except FileNotFoundError:
        print(f"ERROR: JSON database not found: {JSON_PATH}")
        print("Please run 'utilities/csv_to_json_converter.py' first.")
        return
    except Exception as e:
        print(f"ERROR loading JSON: {e}")
        return
    
    # --- 4. Processing ---
    # Find rows where we haven't attempted a search yet
    rows_to_process = df[df['genius_status'].isna()]
    
    if rows_to_process.empty:
        print("All songs have already been processed. Nothing to do.")
        return

    print(f"Found {len(rows_to_process)} new songs to process.")

    # Apply batch size setting if it's set
    if BATCH_SIZE:
        print(f"--- RUNNING IN TEST MODE (BATCH_SIZE = {BATCH_SIZE}) ---")
        rows_to_process = rows_to_process.head(BATCH_SIZE)
        if rows_to_process.empty:
             print("Test batch is empty (all songs in batch already processed).")
             return

    # Use tqdm for a progress bar
    tqdm.pandas(desc="Fetching Genius Data")
    
    # Run the search function on all selected rows
    new_data = rows_to_process.progress_apply(get_genius_data, args=(genius,), axis=1)

    # new_data is now a Series of dictionaries. We need to merge this
    # back into our main DataFrame 'df'.
    
    # Update the main DataFrame 'df' at the correct indices
    df.loc[new_data.index, 'lyrics'] = new_data.apply(lambda x: x['lyrics'])
    df.loc[new_data.index, 'genius_status'] = new_data.apply(lambda x: x['genius_status'])
    df.loc[new_data.index, 'genius_pageviews'] = new_data.apply(lambda x: x['genius_pageviews'])
    df.loc[new_data.index, 'genius_release_date'] = new_data.apply(lambda x: x['genius_release_date'])

    print("\nBatch complete. Saving results to JSON...")

    # --- 5. Save Results ---
    try:
        # Convert DataFrame back to list of dicts
        output_data = df.to_dict(orient='records')
        
        # Save JSON
        with open(JSON_PATH, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=4, ensure_ascii=False)
            
        print(f"Successfully updated: {JSON_PATH}")
        
        # Final Report
        total_songs = len(df)
        success_count = len(df[df['genius_status'] == 'Success'])
        not_found_count = len(df[df['genius_status'] == 'Song Not Found'])
        empty_count = len(df[df['genius_status'] == 'Lyrics Empty'])
        unprocessed_count = len(df[df['genius_status'].isna()])
        
        print("\n--- Genius Scraper Report ---")
        print(f"Total songs:          {total_songs}")
        print(f"Lyrics Found:         {success_count}")
        print(f"Songs Not Found:      {not_found_count}")
        print(f"Found but no Lyrics:  {empty_count}")
        print(f"Still unprocessed:    {unprocessed_count}")

    except Exception as e:
        print(f"ERROR saving JSON: {e}")

if __name__ == "__main__":
    main()