# scripts/main_pipeline.py

import os
import sys
from datetime import datetime
import json

# Add the scripts directory to the Python path
# This allows importing modules from the same directory
script_dir = os.path.dirname(__file__)
sys.path.append(script_dir)

from extract_weather import extract_weather_data
from transform_weather import transform_weather_data
from load_weather import load_weather_data

def run_weather_etl(city: str, country: str):
    """
    Runs the complete ETL pipeline for weather data.

    Args:
        city (str): The name of the city to get weather data for.
        country (str): The 2-letter country code for the city.
    """
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting ETL for {city}, {country}...")

    # 1. Extract Data
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Extracting data...")
    raw_weather_data = extract_weather_data(city_name=city, country_code=country)

    if raw_weather_data:
        # Optional: Save raw data to a daily file for debugging/auditing
        raw_output_dir = os.path.join(script_dir, "..", "data", "raw")
        os.makedirs(raw_output_dir, exist_ok=True)
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        raw_file_path = os.path.join(raw_output_dir, f"raw_weather_{city.lower().replace(' ', '_')}_{timestamp_str}.json")
        try:
            with open(raw_file_path, "w") as f:
                json.dump(raw_weather_data, f, indent=4)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Raw data saved to {raw_file_path}")
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Error saving raw data: {e}")


        # 2. Transform Data
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Transforming data...")
        transformed_df = transform_weather_data(raw_weather_data)

        if not transformed_df.empty:
            # 3. Load Data
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Loading data...")
            load_weather_data(transformed_df, city_name=city)
        else:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Transformation resulted in empty DataFrame. Skipping load.")
    else:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Extraction failed. Skipping transformation and load.")

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ETL finished for {city}, {country}.\n")

if __name__ == "__main__":
    # You can define a list of cities/countries to process
    cities_to_process = [
        {"city": "Chicago", "country": "us"},
        {"city": "London", "country": "gb"},
        {"city": "Tokyo", "country": "jp"}
    ]

    for location in cities_to_process:
        run_weather_etl(location["city"], location["country"])