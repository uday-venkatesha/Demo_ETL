# scripts/extract_weather.py

import requests
import json
import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def extract_weather_data(city_name: str = "Chicago", country_code: str = "us"):
    """
    Extracts current weather data for a given city from OpenWeatherMap API.

    Args:
        city_name (str): The name of the city.
        country_code (str): The 2-letter country code (e.g., 'us', 'gb').

    Returns:
        dict: Raw JSON weather data, or None if an error occurs.
    """
    api_key = os.getenv("OPENWEATHER_API_KEY")
    if not api_key:
        print("Error: OPENWEATHER_API_KEY not found in .env file.")
        return None

    base_url = "http://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": f"{city_name},{country_code}",
        "appid": api_key,
        "units": "metric"  # You can change to 'imperial' for Fahrenheit
    }

    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)
        raw_data = response.json()
        return raw_data
    except requests.exceptions.RequestException as e:
        print(f"Error extracting data for {city_name}: {e}")
        return None

if __name__ == "__main__":
    # Example usage:
    chicago_data = extract_weather_data(city_name="Chicago", country_code="us")
    if chicago_data:
        print("Successfully extracted data for Chicago.")
        # Optional: Save raw data for inspection
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = os.path.join("..", "data", "raw")
        os.makedirs(output_dir, exist_ok=True)
        file_path = os.path.join(output_dir, f"raw_weather_chicago_{timestamp_str}.json")
        with open(file_path, "w") as f:
            json.dump(chicago_data, f, indent=4)
        print(f"Raw data saved to {file_path}")

    london_data = extract_weather_data(city_name="London", country_code="gb")
    if london_data:
        print("\nSuccessfully extracted data for London.")