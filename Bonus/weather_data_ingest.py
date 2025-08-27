import os
import requests
import json
from google.cloud import bigquery

# Set up BigQuery client
client = bigquery.Client()

# Define project and dataset information
PROJECT_ID = 'qwiklabs-gcp-04-db5d757ff012'
DATASET_ID = 'Bonus'
AIRPORTS_TABLE_ID = 'airports' # The table with airport data
FORECASTS_TABLE_ID = 'weather_forecasts' # The new table to store forecasts

def ingest_weather_data(request):
    """
    Orchestrates the data ingestion process.
    This function can be triggered by an HTTP request or other events.
    """
    print(f"Starting weather data ingestion for project: {PROJECT_ID}")

    try:
        # 1. Read the list of large US airports from BigQuery
        airports = get_large_airports_from_bq()
        if not airports:
            print("No large airports found in the BigQuery table. Exiting.")
            return 'No airports found', 200

        # 2. Iterate through each airport and get the weather forecast
        all_forecasts = []
        for airport in airports:
            forecast_data = get_forecast_for_airport(airport)
            if forecast_data:
                all_forecasts.append(forecast_data)
            
        if not all_forecasts:
            print("No forecast data was collected. Exiting.")
            return 'No forecasts collected', 200

        # 3. Load the forecast data into a new BigQuery table
        load_data_to_bq(all_forecasts)

        print(f"Successfully ingested {len(all_forecasts)} weather forecasts.")
        return 'Weather data ingestion complete!', 200

    except Exception as e:
        print(f"An error occurred: {e}")
        return f'An error occurred: {e}', 500

def get_large_airports_from_bq():
    """Reads large airport data from BigQuery."""
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{AIRPORTS_TABLE_ID}"
    query = f"""
        SELECT
          name,
          municipality,
          iso_region,
          latitude_deg,
          longitude_deg
        FROM
          `{table_ref}`
        WHERE
          type = 'large_airport' AND iso_country = 'US'
        LIMIT 10 -- Limiting for a faster proof of concept
    """
    
    print(f"Fetching airport data from: {table_ref}")
    query_job = client.query(query)
    results = [dict(row) for row in query_job]
    print(f"Found {len(results)} large airports.")
    return results

def get_forecast_for_airport(airport):
    """
    Makes API calls to the National Weather Service to get a forecast for a given airport.
    """
    lat = airport['latitude_deg']
    lon = airport['longitude_deg']
    
    # NWS API requires a grid point to get the forecast
    points_url = f"https://api.weather.gov/points/{lat},{lon}"
    print(f"Fetching grid data for {airport['name']} at: {points_url}")
    
    try:
        response = requests.get(points_url, headers={'User-Agent': 'Weather-App/1.0 (contact@example.com)'})
        response.raise_for_status() # Raise an exception for bad status codes
        grid_data = response.json()
        
        forecast_url = grid_data['properties']['forecast']
        print(f"Fetching forecast from: {forecast_url}")
        
        forecast_response = requests.get(forecast_url, headers={'User-Agent': 'Weather-App/1.0 (contact@example.com)'})
        forecast_response.raise_for_status()
        forecast_data = forecast_response.json()
        
        # Extract the current forecast period (usually the first one)
        periods = forecast_data['properties']['periods']
        if not periods:
            print(f"No forecast periods found for {airport['name']}.")
            return None
            
        current_forecast = periods[0]
        
        # Prepare the data for BigQuery
        return {
            "airport_name": airport['name'],
            "city": airport['municipality'],
            "state": airport['iso_region'],
            "temperature": current_forecast['temperature'],
            "wind_speed": current_forecast['windSpeed'],
            "weather_condition": current_forecast['shortForecast'],
            "forecast_timestamp": current_forecast['startTime'],
            "full_forecast_json": json.dumps(forecast_data)
        }

    except requests.exceptions.RequestException as e:
        print(f"API request failed for {airport['name']}: {e}")
        return None
    except KeyError as e:
        print(f"Missing key in API response for {airport['name']}: {e}")
        return None

def load_data_to_bq(data):
    """Loads a list of dictionaries into a BigQuery table."""
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{FORECASTS_TABLE_ID}"
    table = client.get_table(table_ref)
    
    # Load data into BigQuery
    errors = client.insert_rows_json(table, data)
    
    if errors:
        print(f"Errors occurred while loading data: {errors}")
    else:
        print(f"Successfully loaded {len(data)} rows into {table_ref}")

if __name__ == '__main__':
    # This is for local testing purposes.
    # In a Cloud Function, the `ingest_weather_data` function is the entry point.
    ingest_weather_data(None)
