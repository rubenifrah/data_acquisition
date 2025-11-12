import pandas as pd
import os

def convert_csv_to_json(csv_path, json_path):
    """
    read a csv an convert it in the json format
    
    Args:
        csv_path (str)
        json_path (str)
    """
    try:
        print(f"CSV loading from : {csv_path}...")
        df = pd.read_csv(csv_path)
        
        if df.empty:
            print("Error : empty csv")
            return

        print(f"conversion of {len(df)} ligns in JSON...")
        
        # 'orient=records' to create a list of objects : [ {row1}, {row2}, ... ]
        # 'force_ascii=False' to preserve special characters 
        df.to_json(
            json_path,
            orient='records',
            indent=4,
            force_ascii=False
        )
        
        print(f"\success ! JSON saved here : {json_path}")

    except FileNotFoundError:
        print(f"error : file not found : {csv_path}")
    except Exception as e:
        print(f"unexpected error : {e}")

if __name__ == "__main__":
    # paths
    SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
    PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))
    
    
    INPUT_CSV = os.path.join(PROJECT_ROOT, 'data', 'songs_with_spotify_ids.csv')
    
    OUTPUT_JSON = os.path.join(PROJECT_ROOT, 'data', 'songs_database.json')
    
    convert_csv_to_json(INPUT_CSV, OUTPUT_JSON)