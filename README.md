# Building a Dataset for Song Quality Assessment

**Project for: Data Acquisition, Extraction, and Storage (Dauphine IASD)**

**Team: Magalie ZHU, Joey DAVID, Jacques LACHOUQUE, Ruben IFRAH**

This repository contains the code for our group project. The goal is to build a high-quality, enriched dataset of popular music over the years based on the Billboard Year-End charts. This dataset will serve as the foundation for future analysis to answer the question: "Has the 'quality' of popular music decreased over the last 50 years?"

This repository covers **Part 1 (Scraping)** and **Part 2 (Enrichment)** of this pipeline.

---

## Project Structure

This project is organized into distinct directories to separate concerns:

-   **/billboard_scraper/**: A Scrapy project that crawls Wikipedia to get the base song list.
-   **/analysis/**: A collection of Python scripts for cleaning, analyzing, and enriching the data.
-   **/data/**: The destination for all generated CSV files.
-   **/figures/**: The destination for all generated plots.
-   **/.env**: (Ignored by Git) A file to store our secret Spotify API keys.
-   **/requirements.txt**: A list of all Python libraries needed to run this project.

---

## How to Run This Project (Step-by-Step)

### Setup

1.  **Clone the Repository:**
    ```bash
    git clone [https://github.com/rubenifrah/data_acquisition.git](https://github.com/rubenifrah/data_acquisition.git)
    cd data_acquisition
    ```

2.  **Install All Dependencies:**
    It is highly recommended to use a Python virtual environment.
    ```bash
    # Create a virtual environment (optional but recommended)
    python3 -m venv venv
    source venv/bin/activate
    
    # Install all required libraries
    pip install -r requirements.txt
    ```


### Step 0 (skip all other steps): Run the Whole Pipeline in One Shot

Use `pipeline.py` to stitch everything together (audio metadata from Spotify/YouTube fallbacks, YouTube discovery/comments, Wikipedia awards scraping, YAML assembly). It starts from `data/songs_database.json`, which should already contain the Spotify IDs, lyrics, and Wikipedia links.

```bash
python pipeline.py -n 10 \
  --comments 10 \          # optional: top comments to keep (max 15)
  --sample-rate 22050 \    # optional override
  --duration 30            # optional override
```
- Ensure `data/songs_database.json` exists first (produced by the Spotify/lyrics prep).
- `-n`: How many *new* fully processed songs to add, starting from the top of `data/songs_database.json`. If the YAML already has 10 entries and you pass `-n 10`, the pipeline targets the first 20 songs.
- `--comments`: Top N comments per track (or fewer if not available), capped at 15.
- Processing is pipelined per song (audio → YouTube link discovery → comments → awards), so stage 1 of song N+1 starts as soon as song N reaches the next stage. Each stage writes after every song, making long runs resilient to interruptions and naturally rate-limited.
- The pipeline only adds a song if Spotify ID, Genius lyrics, and audio metadata are present; YouTube comments and awards are attached when available (it retries up to 4 YouTube search hits for comments).
- Intermediate files are written to `data/` (audio metadata JSON, discovered YouTube link JSON, comments/awards JSONs). Existing files are reused and updated song-by-song so repeated runs pick up where they left off.
- If the next song in chart order is missing required data (Spotify ID, lyrics, or audio metadata), the pipeline stops instead of skipping, so YAML order stays contiguous.


### Step 1: Scrape Billboard Data

This step uses the Scrapy spider to crawl Wikipedia and build our initial list of ~7,000 songs.

* **Command:** (Run from the root `data_acquisition/billboard_scraper/:` folder)
    ```bash
    scrapy crawl billboard_spider -o data/all_songs.csv
    ```
* **Output:** Creates the `data/all_songs.csv` file.
* **Time:** ~1-2 minutes.

### Step 2: Enrich with Spotify Data

This is the most critical (and longest) step. This script will:
1.  Read `data/all_songs.csv`.
2.  Search Spotify for all ~7,000 songs to get their `spotify_track_id`.
3.  **Save an intermediate file** `data/songs_with_spotify_ids.csv` so you never have to run the 1-hour search again.
4.  Fetch audio features (danceability, energy, etc.) in batches.
5.  Save the final, complete dataset.

**Before you run:**
1.  Get your Spotify Client ID and Secret from the [Spotify Developer Dashboard](https://developer.spotify.com/dashboard).
2.  Create a file named `.env` in the root folder.
3.  Paste your keys into it like this:
    ```.env
    SPOTIPY_CLIENT_ID='YOUR_CLIENT_ID_GOES_HERE'
    SPOTIPY_CLIENT_SECRET='YOUR_CLIENT_SECRET_GOES_HERE'
    ```

* **Command:** (Run from the root folder)
    ```bash
    python analysis/spotify_enrichment.py
    ```
* **Output:** Creates `data/songs_with_spotify_ids.csv` and then `data/songs_with_audio_features.csv`.
* **Time:** **~1 Hour.** This is normal. It will show a progress bar.
    * **If it fails (e.g., 403 Error):** Check your Spotify app's "Users and Access" settings. You can just re-run the command, and it will resume from the `songs_with_spotify_ids.csv` file, skipping the 1-hour search.

### Step 3: Run Analysis & Generate Plots

This script runs on the *original* scraped data (not the enriched data) to check its quality.

* **Command:** (Run from the root folder)
    ```bash
    python analysis/billboard_analysis.py
    ```
* **Output:** Generates several plots (like `songs_per_year.png`) and saves them to the `figures/` folder.

---

### Step 4: Compute Audio Metadata from Spotify Previews

This mirrors the librosa feature extraction used in the scratch neural-net project.

* **Command:** (Run from the root folder)
    ```bash
    python analysis/audio_metadata_enrichment.py \
      --input data/songs_database.json \
      --output data/songs_with_audio_metadata.json \
      --limit 25 \
      --duration 30 \
      --offset 0 \
      --sample-rate 22050 \
      --force
    ```
    - `--input`: Path to the JSON/CSV source dataset, defaulting to `data/songs_database.json`.
    - `--output`: Destination for the enriched dataset (`data/songs_with_audio_metadata.json` by default).
    - `--limit`: Only process the first *N* rows; omit it to run on the entire dataset.
    - `--duration`: Maximum duration in seconds of each clip that Librosa loads (defaults to 30 seconds).
    - `--offset`: Row index to start from so you can chunk the workload across runs.
    - `--sample-rate`: Target sampling rate fed to Librosa (default 22,050 Hz).
    - `--force`: Recompute metadata for rows that already have `audio_metadata`; drop the flag to skip completed rows.
* **Notes:** Requires the same Spotify credentials as Step 2. The script pulls the preview URL for each track, downloads the clip, computes features such as MFCCs and tempo, and stores them under `audio_metadata`.
  When a Spotify preview is missing, it will fall back to a YouTube search and compute the features from the first audio result.

### Step 5: Optional Scrapers for Comments and Awards

First, auto-discover the target pages (no hand-made JSONs needed):
```bash
python analysis/discover_links.py \
  --input data/all_songs.csv \
  --limit 100 \                    # optional: first N rows (after offset)
  --offset 0 \                     # optional: skip N rows
  --youtube-output data/youtube_links.json \
  --wiki-output data/wiki_awards_links.json \
  --force-wiki-search \            # optional: ignore existing 'link' column
  --force-youtube-search           # optional: ignore any youtube_url/audio_preview_url columns
```
- `--skip-youtube` / `--skip-wiki`: Disable either search pass.
- Defaults reuse any existing Wikipedia `link` column and any YouTube URLs already present (e.g., `audio_preview_url` from the audio metadata fallback).
- YouTube discovery stores up to the top 4 search candidates (`youtube_candidates`) so the spider can fall back if one video has no comments.

Then run the Scrapy spiders:

* **YouTube comments (top 10 liked):**
    ```bash
    scrapy crawl youtube_comments \
      -a links_path=data/youtube_links.json \
      -a limit=100 \                # optional: only first N rows from links_path
      -a max_comments=10 \          # optional: number of comments per video
      -O data/youtube_comments.json
    ```
    `data/youtube_links.json` should contain objects with `name`, `artist`, and either `youtube_id` or `youtube_url`.
    The spider pulls commentEntityPayload from the YouTube API (works with current responses), respecting `max_comments`. It sets a browser user-agent and skips robots.txt for comment calls. If the first video has zero comments or fails, it automatically retries the 2nd–4th candidates from `youtube_candidates`; after that it records no comments.

* **Wikipedia awards/recognition:**
    ```bash
    scrapy crawl wikipedia_awards \
      -a dataset_path=data/wiki_awards_links.json \
      -a limit=100 \                # optional: only first N rows from dataset_path
      -O data/wikipedia_awards.json
    ```
    This spider scans each song's Wikipedia page (with a browser user-agent) and captures award-ish content from paragraphs, lists, and tables (Grammy/award/nomination/ranking mentions). Output entries include `track_name`, `artist`, `year`, `source`, and `awards` (list of strings).

### Step 6: Export the Final YAML Dataset

Combine all available enrichments into a single YAML file.

```bash
python analysis/build_yaml_dataset.py \
  --input data/songs_database.json \
  --audio-metadata data/songs_with_audio_metadata.json \
  --youtube-comments data/youtube_comments.json \
  --awards data/wikipedia_awards.json \
  --output data/songs_dataset.yaml
```

The YAML output keeps song identity, Spotify metadata, computed audio descriptors, lyrics, and placeholders for YouTube comments and Wikipedia awards.
