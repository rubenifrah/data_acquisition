import pandas as pd
import matplotlib.pyplot as plt
import os
import json
import warnings

# Suppress warnings
warnings.filterwarnings("ignore", module="matplotlib.font_manager")
pd.options.mode.chained_assignment = None # Suppress pandas copy warning

def simplify_status(status):
    """
    Simplifies the detailed genius_status into clean categories for plotting.
    """
    if pd.isna(status):
        return 'Error / Pending'
    if status.startswith('Success'):
        return 'Success (Lyrics Found)'
    if 'Lyrics Empty' in status:
        return 'Found (No Lyrics)'
    if 'Song Not Found' in status:
        return 'Song Not Found'
    if 'Ambiguous' in status:
        return 'Ambiguous Match'
    if 'Error' in status:
        return 'Error / Pending'
    return 'Other'

def analyze_genius_results(json_path):
    """
    Loads the enriched JSON database and generates plots
    to analyze the success of the Genius lyrics scraping.
    """
    
    # --- 1. Load Data ---
    try:
        print(f"Loading database from: {json_path}...")
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        df = pd.DataFrame(data)
        print(f"Loaded {len(df)} total songs.")
    except FileNotFoundError:
        print(f"🆘 ERROR: JSON database not found at {json_path}")
        return
    except Exception as e:
        print(f"🆘 ERROR loading JSON: {e}")
        return
        
    # --- 2. Data Preparation ---
    
    # Ensure 'year' is numeric for sorting
    df['year'] = pd.to_numeric(df['year'], errors='coerce')
    df = df.dropna(subset=['year']) # Drop rows where year is unknown
    df['year'] = df['year'].astype(int)

    # Create the new simplified 'plot_status' column
    df['plot_status'] = df['genius_status'].apply(simplify_status)
    
    # --- 3. Overall Report (Console) ---
    total_songs = len(df)
    found_count = len(df[df['plot_status'] == 'Success (Lyrics Found)'])
    match_rate = (found_count / total_songs) * 100
    
    print("\n--- 📊 Genius Scraper Report ---")
    print(f"Total Songs in Database: {total_songs}")
    print(f"Songs WITH Lyrics Found: {found_count}")
    print(f"Overall Match Rate:      {match_rate:.2f}%")
    
    print("\n--- Detailed `genius_status` Breakdown ---")
    print(df['genius_status'].value_counts(dropna=False).to_string())
    print("\n--- Simplified Plotting Categories ---")
    print(df['plot_status'].value_counts(dropna=False).to_string())
    print("---------------------------------------")

    
    # --- 4. Plot 1: Standard Stacked Bar Chart (The New Plot) ---
    print("\n📊 Generating Stacked Bar Chart (by Count)...")
    
    # Use crosstab to get counts of each status per year
    # Rows = year, Columns = status, Values = count
    status_counts = pd.crosstab(df['year'], df['plot_status'])

    # Define a logical color map
    color_map = {
        'Success (Lyrics Found)': 'forestgreen',
        'Found (No Lyrics)': 'yellowgreen',
        'Ambiguous Match': 'lightcoral',
        'Song Not Found': 'lightcoral',
        'Error / Pending': 'slategray'
    }
    
    # Re-order columns for a logical stack (success on bottom, fail on top)
    plot_order = [
        'Success (Lyrics Found)', 
        'Found (No Lyrics)', 
        'Ambiguous Match',
        'Song Not Found',
        'Error / Pending'
    ]
    
    # Filter the DataFrame to only include columns that actually exist in our data
    final_columns = [col for col in plot_order if col in status_counts.columns]
    final_colors = [color_map[col] for col in final_columns]
    
    # --- THIS IS THE KEY CHANGE ---
    # We plot 'status_counts' (the raw numbers) directly.
    # We are NO LONGER normalizing to 100%.
    fig, ax = plt.subplots(figsize=(20, 10))
    
    status_counts[final_columns].plot(
        kind='bar',
        stacked=True,  # This will stack the counts
        color=final_colors,
        width=0.8,
        ax=ax
    )
    
    # We NO LONGER format the Y-axis as a percentage.
    # It is now a simple count.
    
    plt.title('Genius match status per year', fontsize=18)
    plt.xlabel('year', fontsize=12)
    plt.ylabel('number of songs', fontsize=12) # Y-axis is now "Number of Songs"
    
    # Set Y-axis limit to be slightly above the max, e.g., 110
    # (since Billboard charts are often 100)
    max_height = status_counts.sum(axis=1).max()
    ax.set_ylim(0, max_height * 1.1)
    
    # Place legend outside the plot
    plt.legend(
        title='Match Status', 
        bbox_to_anchor=(1.02, 1), 
        loc='upper left', 
        fontsize=12
    )
    
    plt.xticks(rotation=90, fontsize=8)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    
    # Adjust layout to prevent legend from being cut off
    plt.tight_layout(rect=[0, 0, 0.85, 1]) # Make room for legend
    
    # --- 5. Show the Plot ---
    print("Displaying plot. Close the plot window to exit.")
    plt.show()

def main():
    # Build robust paths
    SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
    PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))
    
    JSON_PATH = os.path.join(PROJECT_ROOT, 'data', 'songs_database.json')
        
    analyze_genius_results(JSON_PATH)

if __name__ == "__main__":
    main()