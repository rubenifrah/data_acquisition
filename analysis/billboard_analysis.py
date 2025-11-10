import pandas as pd
import matplotlib.pyplot as plt
import os # <-- Import the 'os' library to manage file paths

def analyze_billboard_data():
    """
    Loads, cleans, and analyzes 'all_songs.csv' from the '../data/' folder.
    Saves plots to the '../figures/' folder and displays them.
    """
    # --- Configuration ---
    pd.set_option('display.max_columns', None)

    # --- Define relative paths ---
    # Get the directory where this script (billboard_analysis.py) is located
    SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
    
    # Go up one level to the project root (data_acquisition/)
    PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))
    
    # Define paths to the data and figures directories
    DATA_FILE = os.path.join(PROJECT_ROOT, 'data', 'all_songs.csv')
    FIGURES_DIR = os.path.join(PROJECT_ROOT, 'figures')

    # Ensure the figures directory exists
    os.makedirs(FIGURES_DIR, exist_ok=True)


    # --- Load Data ---
    # Use the new DATA_FILE path
    try:
        df = pd.read_csv(DATA_FILE)
        print(f"Successfully loaded '{DATA_FILE}'.")
        print(f"Total rows: {len(df)}")
    except FileNotFoundError:
        print(f"ERROR: File '{DATA_FILE}' not found.")
        print("Please run the Scrapy spider from the root folder:")
        print("scrapy crawl billboard_spider -o data/all_songs.csv")
        return  # Stop the script if file not found

    if df.empty:
        print("The CSV file is empty. Halting analysis.")
        return

    # Display first 5 rows for a quick check
    print("\n--- Data Preview ---")
    print(df.head())

    # --- Initial Data Inspection ---
    print("\n--- Initial Inspection (Data Types) ---")
    df.info()

    # --- Data Cleaning ---
    print("\n--- Data Cleaning ---")
    df_cleaned = df.copy()
    
    # Convert 'year' and 'place' to numeric
    df_cleaned['year'] = pd.to_numeric(df_cleaned['year'], errors='coerce')
    df_cleaned['place'] = pd.to_numeric(df_cleaned['place'], errors='coerce')

    # Drop rows where conversion failed
    original_rows = len(df_cleaned)
    df_cleaned = df_cleaned.dropna(subset=['year', 'place'])
    cleaned_rows = len(df_cleaned)
    
    print(f"Rows before cleaning: {original_rows}")
    print(f"Invalid rows removed: {original_rows - cleaned_rows}")
    print(f"Rows after cleaning: {cleaned_rows}")

    # Convert to integer for cleaner display
    df_cleaned['year'] = df_cleaned['year'].astype(int)
    df_cleaned['place'] = df_cleaned['place'].astype(int)
    
    print("\nData types after cleaning:")
    df_cleaned.info()

    # --- 4. Integrity Checks & Further Analysis ---
    print("\n--- Integrity Checks & Analysis ---")
    
    # Check for missing values (especially 'link')
    print("\nMissing values per column (after cleaning):")
    missing_values = df_cleaned.isnull().sum()
    print(missing_values)
    print("\nNote: It is NORMAL to have missing 'link' values.")

    # Top Artists and Songs for plotting
    top_artists = df_cleaned['artist'].value_counts().head(15)
    print("\n--- Top 15 Most Frequent Artists ---")
    print(top_artists)

    top_songs = df_cleaned['name'].value_counts().head(15)
    print("\n--- Top 15 Most Frequent Song Names ---")
    print("(Useful for seeing common reprises or standards)")
    print(top_songs)

    # --- Plotting ---
    print("\n--- Generating and Saving Plots ---")
    
    # === PLOT 1: Song Count per Year ===
    songs_per_year = df_cleaned['year'].value_counts().sort_index().reset_index()
    songs_per_year.columns = ['year', 'song_count']
    
    plt.figure(figsize=(20, 10))
    plt.bar(songs_per_year['year'], songs_per_year['song_count'])
    plt.title('Number of Songs Scraped per Year', fontsize=16)
    plt.xlabel('Year', fontsize=12)
    plt.ylabel('Number of Songs', fontsize=12)
    plt.xticks(rotation=90, fontsize=8)
    plt.yticks(fontsize=10)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()

    # --- NEW: Save the plot ---
    plot1_path = os.path.join(FIGURES_DIR, 'songs_per_year.png')
    plt.savefig(plot1_path)
    print(f"Saved plot: {plot1_path}")

    # === PLOT 2: Top 15 Artists ===
    plt.figure(figsize=(12, 8))
    # Use barh (horizontal) for long labels
    top_artists.plot(kind='barh', color='skyblue')
    plt.title('Top 15 Most Frequent Artists in Year-End Charts', fontsize=16)
    plt.xlabel('Number of Appearances', fontsize=12)
    plt.ylabel('Artist', fontsize=12)
    plt.gca().invert_yaxis() # Puts the #1 artist at the top
    plt.grid(axis='x', linestyle='--', alpha=0.7)
    plt.tight_layout()

    # --- Save the plot ---
    plot2_path = os.path.join(FIGURES_DIR, 'top_15_artists.png')
    plt.savefig(plot2_path)
    print(f"Saved plot: {plot2_path}")


    # --- 6. Show All Plots ---
    print("\nDisplaying all plots. Close the plot windows to exit.")
    plt.show()

if __name__ == "__main__":
    analyze_billboard_data()