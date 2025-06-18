# scripts/transform_weather.py

import pandas as pd
from datetime import datetime

def transform_weather_data(raw_data: dict) -> pd.DataFrame:
    """
    Transforms raw OpenWeatherMap JSON data into a clean Pandas DataFrame.

    Args:
        raw_data (dict): The raw JSON data extracted from OpenWeatherMap.

    Returns:
        pd.DataFrame: A DataFrame with processed weather information.
                      Returns an empty DataFrame if raw_data is None or invalid.
    """
    if not raw_data:
        print("No raw data provided for transformation.")
        return pd.DataFrame()

    try:
        # Extract main weather details
        main_data = raw_data.get('main', {})
        weather_details = raw_data.get('weather', [{}])[0] # Get first weather description
        wind_data = raw_data.get('wind', {})
        sys_data = raw_data.get('sys', {})

        # Create a dictionary for the transformed row
        transformed_row = {
            'city_name': raw_data.get('name'),
            'country_code': sys_data.get('country'),
            'latitude': raw_data.get('coord', {}).get('lat'),
            'longitude': raw_data.get('coord', {}).get('lon'),
            'temperature_celsius': main_data.get('temp'), # 'units=metric' returns Celsius
            'feels_like_celsius': main_data.get('feels_like'),
            'min_temp_celsius': main_data.get('temp_min'),
            'max_temp_celsius': main_data.get('temp_max'),
            'pressure_hPa': main_data.get('pressure'),
            'humidity_percent': main_data.get('humidity'),
            'sea_level_pressure_hPa': main_data.get('sea_level'),
            'ground_level_pressure_hPa': main_data.get('grnd_level'),
            'wind_speed_mps': wind_data.get('speed'), # 'units=metric' returns m/s
            'wind_direction_deg': wind_data.get('deg'),
            'clouds_percent': raw_data.get('clouds', {}).get('all'),
            'weather_main': weather_details.get('main'),
            'weather_description': weather_details.get('description'),
            'visibility_meters': raw_data.get('visibility'),
            'timestamp_utc': datetime.fromtimestamp(raw_data.get('dt')),
            'sunrise_utc': datetime.fromtimestamp(sys_data.get('sunrise')),
            'sunset_utc': datetime.fromtimestamp(sys_data.get('sunset')),
            'data_ingestion_time_utc': datetime.utcnow() # When we processed it
        }

        # Handle potential missing keys gracefully by setting to None if not found
        for key, value in transformed_row.items():
            if isinstance(value, dict): # Handle nested dicts if any
                transformed_row[key] = None
            elif value is None and key not in ['sea_level_pressure_hPa', 'ground_level_pressure_hPa']:
                # Print a warning for critical missing fields if needed
                # print(f"Warning: Missing key '{key}' in raw data for {raw_data.get('name')}")
                pass

        df = pd.DataFrame([transformed_row])

        # Convert timestamp columns to datetime objects if they aren't already
        for col in ['timestamp_utc', 'sunrise_utc', 'sunset_utc', 'data_ingestion_time_utc']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], unit='s', errors='coerce')

        return df

    except Exception as e:
        print(f"Error transforming data: {e}")
        print(f"Raw data causing error: {raw_data}")
        return pd.DataFrame()

if __name__ == "__main__":
    # This is just for testing the transform function in isolation
    # In a real scenario, you'd load raw data from a file or directly from extract_weather.py
    sample_raw_data = {
        "coord": {"lon": -87.65, "lat": 41.85},
        "weather": [{"id": 800, "main": "Clear", "description": "clear sky", "icon": "01d"}],
        "base": "stations",
        "main": {
            "temp": 25.5,
            "feels_like": 26.1,
            "temp_min": 24.0,
            "temp_max": 27.0,
            "pressure": 1012,
            "humidity": 60,
            "sea_level": 1012, # Example of optional field
            "grnd_level": 990 # Example of optional field
        },
        "visibility": 10000,
        "wind": {"speed": 4.1, "deg": 230},
        "clouds": {"all": 0},
        "dt": 1678886400, # Example timestamp (March 15, 2023 12:00:00 PM UTC)
        "sys": {"type": 2, "id": 2005459, "country": "US", "sunrise": 1678867200, "sunset": 1678910400},
        "timezone": -18000,
        "id": 4887398,
        "name": "Chicago",
        "cod": 200
    }
    transformed_df = transform_weather_data(sample_raw_data)
    if not transformed_df.empty:
        print("\nTransformed DataFrame:")
        print(transformed_df)
        print("\nDataFrame Info:")
        transformed_df.info()