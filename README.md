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

### Step 0: Setup

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

## Next Steps (Project Scope)

The output of this repository is the `data/songs_with_audio_features.csv` file. This file is the starting point for the next phases of our project, which will include:

-   **Phase 3: Genius API:** Scrape lyrics for all matched songs.
-   **Phase 4: YouTube API:** Get public reception data (views, likes, comments).
-   **Phase 5: Final Database:** Merge all sources into a single MongoDB database, with each song as a document.