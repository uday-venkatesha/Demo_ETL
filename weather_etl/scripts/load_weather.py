# scripts/load_weather.py

import pandas as pd
import os
from datetime import datetime

def load_weather_data(df: pd.DataFrame, city_name: str = "Chicago"):
    """
    Loads the transformed weather DataFrame into a CSV file.

    Args:
        df (pd.DataFrame): The DataFrame containing transformed weather data.
        city_name (str): The name of the city, used for file naming.
    """
    if df.empty:
        print("No data to load. DataFrame is empty.")
        return

    output_dir = os.path.join("..", "data", "processed")
    os.makedirs(output_dir, exist_ok=True)

    # Use a daily file name for easier management and avoid overwriting
    today_str = datetime.now().strftime("%Y-%m-%d")
    file_name = f"{city_name.lower().replace(' ', '_')}_weather_daily_{today_str}.csv"
    file_path = os.path.join(output_dir, file_name)

    # Check if the file already exists to decide whether to write header
    file_exists = os.path.exists(file_path)

    try:
        # Append if file exists, otherwise write new file with header
        df.to_csv(file_path, mode='a', header=not file_exists, index=False)
        print(f"Data successfully loaded to: {file_path}")
    except Exception as e:
        print(f"Error loading data to CSV: {e}")

if __name__ == "__main__":
    # This is just for testing the load function in isolation
    # Create a dummy DataFrame
    dummy_data = {
        'city_name': ['TestCity'],
        'country_code': ['TC'],
        'temperature_celsius': [20.0],
        'humidity_percent': [70],
        'timestamp_utc': [datetime.utcnow()],
        'data_ingestion_time_utc': [datetime.utcnow()]
    }
    dummy_df = pd.DataFrame(dummy_data)
    print("Loading dummy data...")
    load_weather_data(dummy_df, city_name="TestCity")