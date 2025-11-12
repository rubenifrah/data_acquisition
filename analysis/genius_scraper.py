import pandas as pd
import lyricsgenius
import time
import os
import re
from dotenv import load_dotenv
from tqdm import tqdm
import json
from thefuzz import fuzz
import requests

# --- SETTINGS ---
# Set to a small number (e.g., 10) to test on the first 10 *unprocessed* songs.
# Set to None to run the full script on all remaining songs.
BATCH_SIZE = 6000
# How often to save progress to the JSON file
SAVE_INTERVAL = 25
# How many times to retry a failed network request
RETRY_ATTEMPTS = 7
# How long to wait between retries (in seconds)
RETRY_BACKOFF_FACTOR = 10
# Fuzzy match threshold for Step 2
ARTIST_FUZZY_THRESHOLD = 70
# Year window for Step 3
YEAR_WINDOW = 4
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
        print("🆘 ERROR: GENIUS_ACCESS_TOKEN not found in .env file.")
        print("Please create an account at genius.com/api-clients and get a token.")
        return None
        
    try:
        genius = lyricsgenius.Genius(genius_token,
                                     timeout=30,
                                     retries=RETRY_ATTEMPTS, # Use built-in retry
                                     verbose=False, 
                                     remove_section_headers=True,
                                     skip_non_songs=True,
                                     sleep_time=0.5)
        print("✅ Genius client successfully authenticated.")
        return genius
    except Exception as e:
        print(f"🆘 Error authenticating Genius: {e}")
        return None

def clean_lyrics(lyrics_text):
    """
    Cleans the raw lyrics text retrieved from Genius.
    """
    if not isinstance(lyrics_text, str):
        return None
    
    lyrics_text = re.sub(r'^\d*EmbedShare URLCopyEmbedCopy', '', lyrics_text.strip())
    lyrics_text = re.sub(r'\d+ Contributors.*$', '', lyrics_text.strip())
    lyrics_text = re.sub(r'\n+', '\n', lyrics_text.strip())
    
    if not lyrics_text:
        return None
    return lyrics_text

def clean_search_string(text):
    """
    Cleans a song title or artist name for a more flexible search.
    Removes text in parentheses () and brackets [].
    Removes " - Remaster" type suffixes.
    """
    if not isinstance(text, str):
        return ""
    text = re.sub(r'\(.*?\)', '', text) # Remove content in parentheses
    text = re.sub(r'\[.*?\]', '', text) # Remove content in brackets
    text = text.split(' - ')[0] # Remove " - Remaster"
    text = text.strip()
    return text

def parse_genius_year(date_dict):
    """
    NEW: Safely extracts the year (YYYY) from a Genius release_date_components dict.
    e.g., {'year': 1946, 'month': 1, 'day': 1} -> 1946
    """
    if not date_dict or not isinstance(date_dict, dict):
        return None
    return date_dict.get('year')

def get_song_details(genius_client, song_id):
    """
    Helper function to get the full Song object (with lyrics) from a song ID.
    This is the second API call in our new cascade.
    """
    try:
        song = genius_client.song(song_id)
        if song:
            lyrics = clean_lyrics(song.get('lyrics', '')) # Safely get lyrics
            status = "Success" if lyrics else "Lyrics Empty"
            return {
                'lyrics': lyrics,
                'status_code': status,
                'pageviews': song.get('stats', {}).get('pageviews'),
                'release_date': song.get('release_date')
            }
    except Exception as e:
        print(f"  🆘 Error in get_song_details (ID: {song_id}): {e}")
        
    return {
        'lyrics': None,
        'status_code': "Error (Code)",
        'pageviews': None,
        'release_date': None
    }


def get_genius_data(row, genius_client):
    """
    Function to apply to each row of the DataFrame.
    Implements the NEW 3-Step Search Cascade to find the best song match.
    
    Returns a dictionary with all new data.
    """
    song_name = row['name']
    artist_name = row['artist']
    our_year = row['year']
    
    cleaned_song_name = clean_search_string(song_name)
    cleaned_artist_name = clean_search_string(artist_name)
    
    print(f"  (Cleaned: '{cleaned_song_name}' | '{cleaned_artist_name}')")
    
    default_response = {
        'lyrics': None,
        'genius_status': "🆘 Unknown Error",
        'genius_pageviews': None,
        'genius_release_date': None,
        'verbose_status': "🆘 Unknown Error"
    }
    
    attempts = RETRY_ATTEMPTS
    while attempts > 0:
        try:
            # --- Step 1: Strict Search ---
            song_obj = genius_client.search_song(cleaned_song_name, cleaned_artist_name)
            
            if song_obj:
                lyrics = clean_lyrics(song_obj.lyrics)
                status_code = "Success (Strict)" if lyrics else "Lyrics Empty (Strict)"
                verbose = f"  ✅ Success (Strict): Found '{song_obj.title}'"
                
                return {
                    'lyrics': lyrics,
                    'genius_status': status_code,
                    'genius_pageviews': song_obj.pageviews if hasattr(song_obj, 'pageviews') else None,
                    'genius_release_date': song_obj.release_date if hasattr(song_obj, 'release_date') else None,
                    'verbose_status': verbose
                }

            # --- Step 1 Failed. Move to Step 2 (Multi-Candidate Search) ---
            response = genius_client.search_songs(cleaned_song_name)
            hits = response.get('hits', [])

            if not hits:
                return {**default_response, 'genius_status': "Song Not Found", 'verbose_status': "  ⚠️ Song Not Found on Genius."}

            # --- Step 3: Scoring Heuristic ---
            candidates = []
            for hit in hits:
                if hit['type'] != 'song':
                    continue
                
                result = hit['result']
                hit_artist_name = result.get('artist_names', '')
                hit_release_date_parts = result.get('release_date_components')
                hit_year = parse_genius_year(hit_release_date_parts)
                
                # Calculate scores
                artist_score = fuzz.token_set_ratio(cleaned_artist_name, clean_search_string(hit_artist_name))
                year_score = 0
                if hit_year:
                    if abs(hit_year - our_year) <= YEAR_WINDOW:
                        year_score = 100
                    elif abs(hit_year - our_year) <= 5:
                        year_score = 50

                candidates.append({
                    'id': result.get('id'),
                    'title': result.get('title'),
                    'artist': hit_artist_name,
                    'artist_score': artist_score,
                    'year_score': year_score,
                    'pageviews': result.get('stats', {}).get('pageviews')
                })
            
            if not candidates:
                 return {**default_response, 'genius_status': "Song Not Found", 'verbose_status': "  ⚠️ Song Not Found (No valid candidates)."}

            # --- Apply Logic A (Artist-Fuzz) ---
            best_artist_match = max(candidates, key=lambda c: c['artist_score'])
            
            if best_artist_match['artist_score'] > ARTIST_FUZZY_THRESHOLD:
                verbose = f"  ✅ Success (Artist-Fuzz): Matched '{best_artist_match['artist']}' (Score: {best_artist_match['artist_score']})"
                details = get_song_details(genius_client, best_artist_match['id'])
                return {
                    'lyrics': details['lyrics'],
                    'genius_status': f"{details['status_code']} (Artist-Fuzz)",
                    'genius_pageviews': details['pageviews'],
                    'genius_release_date': details['release_date'],
                    'verbose_status': verbose
                }
            
            # --- Apply Logic B (Heuristic) ---
            year_matches = [c for c in candidates if c['year_score'] > 0]
            
            if not year_matches:
                verbose = f"  ❌ Ambiguous Match: No candidates found within year window. Best artist match was '{best_artist_match['artist']}' (Score: {best_artist_match['artist_score']})"
                return {**default_response, 'genius_status': "Ambiguous Match (No Year)", 'verbose_status': verbose}

            # From the songs in the right year, pick the most popular one
            best_heuristic_match = max(year_matches, key=lambda c: c['pageviews'] or 0)
            verbose = f"  ✅ Success (Heuristic): Matched '{best_heuristic_match['artist']}' (Year match, Pageviews: {best_heuristic_match['pageviews']})"
            details = get_song_details(genius_client, best_heuristic_match['id'])
            
            return {
                'lyrics': details['lyrics'],
                'genius_status': f"{details['status_code']} (Heuristic)",
                'genius_pageviews': details['pageviews'],
                'genius_release_date': details['release_date'],
                'verbose_status': verbose
            }
                
        except requests.exceptions.Timeout as e:
            attempts -= 1
            print(f"  ⏳ Timeout Error: {e}. Retrying ({attempts} left)...")
            time.sleep(RETRY_BACKOFF_FACTOR * (RETRY_ATTEMPTS - attempts))
            if attempts == 0:
                return {**default_response, 'genius_status': "Error (Timeout)", 'verbose_status': "  🆘 Error: Timeout after retries."}
        
        except requests.exceptions.RequestException as e:
            attempts -= 1
            print(f"  ⏳ Network/HTTP Error: {e}. Retrying ({attempts} left)...")
            time.sleep(RETRY_BACKOFF_FACTOR * (RETRY_ATTEMPTS - attempts))
            if attempts == 0:
                return {**default_response, 'genius_status': "Error (Network)", 'verbose_status': "  🆘 Error: Network/HTTP error after retries."}
        
        except Exception as e:
            # Catch all other unexpected errors
            print(f"  🆘 Unhandled Error: {e}")
            return {**default_response, 'genius_status': "Error (Code)", 'verbose_status': f"  🆘 Error (Code): {e}"}

def save_json_data(df, path):
    """
    Helper function to save the DataFrame to our JSON file.
    """
    try:
        output_data = df.to_dict(orient='records')
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=4, ensure_ascii=False)
        return True
    except Exception as e:
        print(f"\n🆘 ERROR saving JSON: {e}\n")
        return False

def main():
    """
    Main script to enrich the JSON database with lyrics from Genius.
    """
    # --- 1. Path Configuration ---
    SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
    PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))
    
    JSON_PATH = os.path.join(PROJECT_ROOT, 'data', 'songs_database.json')

    # --- 2. Initialize Client ---
    genius = get_genius_client()
    if not genius:
        return

    # --- 3. Load Data ---
    try:
        print(f"Loading database from: {JSON_PATH}...")
        with open(JSON_PATH, 'r', encoding='utf-8') as f:
            all_songs_data = json.load(f)
        
        df = pd.DataFrame(all_songs_data)
        
        print(f"Loaded {len(df)} songs.")
        
        if 'genius_status' not in df.columns:
            print("Initializing new Genius columns...")
            df['genius_status'] = pd.NA
            df['lyrics'] = pd.NA
            df['genius_pageviews'] = pd.NA
            df['genius_release_date'] = pd.NA
            
    except FileNotFoundError:
        print(f"🆘 ERROR: JSON database not found: {JSON_PATH}")
        print("Please run 'utilities/csv_to_json_converter.py' first.")
        return
    except Exception as e:
        print(f"🆘 ERROR loading JSON: {e}")
        return
    
    # --- 4. Find Rows to Process ---
    
    # A row needs processing if its status is N/A (empty) OR it's an error status
    unprocessed_mask = (
        df['genius_status'].isna() | 
        df['genius_status'].str.contains("Error", na=False)
    )
    rows_to_process = df[unprocessed_mask]
    
    if rows_to_process.empty:
        print("✅ All songs have already been processed successfully. Nothing to do.")
        return

    print(f"Found {len(rows_to_process)} songs that need processing.")

    # --- 5. Apply Batch Size ---
    
    if BATCH_SIZE:
        print(f"--- 🚀 RUNNING IN TEST MODE (BATCH_SIZE = {BATCH_SIZE}) ---")
        rows_to_process = rows_to_process.head(BATCH_SIZE)
        if rows_to_process.empty:
             print("Test batch is empty. Nothing to do.")
             return

    # --- 6. Processing Loop ---
    
    indices_to_process = rows_to_process.index
    total_to_process = len(indices_to_process)
    
    print(f"Starting to process {total_to_process} songs...")
    
    for i, index in enumerate(tqdm(indices_to_process, desc="Processing Songs")):
        
        row = df.loc[index]
        
        print(f"\n🔍 [{i+1}/{total_to_process}] Searching: '{row['name']}' by '{row['artist']}'")
        
        new_data_dict = get_genius_data(row, genius)
        
        print(new_data_dict['verbose_status'])

        df.loc[index, 'lyrics'] = new_data_dict['lyrics']
        df.loc[index, 'genius_status'] = new_data_dict['genius_status']
        df.loc[index, 'genius_pageviews'] = new_data_dict['genius_pageviews']
        df.loc[index, 'genius_release_date'] = new_data_dict['genius_release_date']
        
        if (i + 1) % SAVE_INTERVAL == 0 and (i + 1) < total_to_process:
            print(f"\n💾 Saving progress ({i+1}/{total_to_process})...")
            save_json_data(df, JSON_PATH)
    
    # --- 7. Final Save ---
    print("\n✅ Batch complete. Saving all results...")
    save_json_data(df, JSON_PATH)
            
    print(f"Successfully updated: {JSON_PATH}")
    
    print("\n--- 📊 Genius Scraper Report ---")
    print("Final Status Counts:")
    print(df['genius_status'].value_counts(dropna=False))

if __name__ == "__main__":
    main()