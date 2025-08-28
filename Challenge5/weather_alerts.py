# Import the necessary Google Cloud and other libraries
import os
import time
import requests
import json
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# --- User-Defined Constants ---
PROJECT_ID = "qwiklabs-gcp-04-db5d757ff012"
BQ_DATASET_ID = "Lab5"
BQ_TABLE_ID_AIRPORTS = "weather-data"
BQ_TABLE_ID_ALERTS = "weather_alerts"

# --- Gemini API Configuration ---
API_KEY = "AIzaSyCDjBcBWuDq7SSj3qCQAR3Ac7ntsIdtApI"
API_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-05-20:generateContent?key=" + API_KEY
HEADERS = {'Content-Type': 'application/json'}

def get_large_us_airports():
    """Queries BigQuery for a list of large US airports."""
    print("Fetching list of large U.S. airports from BigQuery...")
    bigquery_client = bigquery.Client()
    try:
        query = f"""
            SELECT
                iata_code,
                latitude_deg,
                longitude_deg,
                municipality
            FROM
                `{PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID_AIRPORTS}`
            WHERE
                type = 'large_airport' AND iso_country = 'US' AND iata_code IS NOT NULL
            LIMIT 10
        """ # Limiting to 10 for a quick proof of concept
        query_job = bigquery_client.query(query)
        results = [dict(row) for row in query_job.result()]
        print(f"Found {len(results)} large airports.")
        return results
    except Exception as e:
        print(f"Error querying BigQuery: {e}")
        print("Please ensure the 'airports' table exists in your specified project and dataset.")
        return []

def get_weather_forecast(lat, lon, iata_code):
    """Fetches the weather forecast for a given latitude and longitude from the NWS API."""
    try:
        # Step 1: Get the forecast URL from the lat/lon
        points_url = f"https://api.weather.gov/points/{lat},{lon}"
        # The NWS API requires a User-Agent header
        response = requests.get(points_url, headers={'User-Agent': 'Weather-Alert-Generator'})
        response.raise_for_status()
        forecast_url = response.json()['properties']['forecast']

        # Step 2: Get the detailed forecast from the URL
        response = requests.get(forecast_url, headers={'User-Agent': 'Weather-Alert-Generator'})
        response.raise_for_status()
        forecast_data = response.json()

        # Extract the detailed forecast for the current period
        detailed_forecast = forecast_data['properties']['periods'][0]['detailedForecast']
        return detailed_forecast
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error for airport {iata_code}: {e}")
        return None
    except Exception as e:
        print(f"Error fetching forecast for airport {iata_code}: {e}")
        return None

def generate_alert_with_gemini(location, forecast_text):
    """Generates a weather alert using the Gemini API."""
    if not API_KEY:
        print("API key is missing. Cannot generate alert.")
        return "Failed to generate alert due to missing API key."

    prompt = f"""
    Create a professional and concise weather alert for {location} based on the following forecast.
    Focus on key elements like temperature, wind, and conditions.
    Forecast: {forecast_text}
    """
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.2
        }
    }

    for i in range(5):
        try:
            response = requests.post(API_URL, headers=HEADERS, json=payload)
            response.raise_for_status()
            result = response.json()
            # Extract the generated text from the nested JSON
            generated_text = result['candidates'][0]['content']['parts'][0]['text']
            return generated_text
        except requests.exceptions.RequestException as e:
            if response.status_code == 403:
                print(f"API Error generating alert: {response.status_code} Forbidden. Check your API key and permissions.")
                return "Failed to generate alert. Forbidden."
            if response.status_code == 429:
                sleep_time = 2 ** i
                print(f"Rate limit exceeded. Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
            else:
                print(f"API Error generating alert for {location}: {e}")
                return "Failed to generate alert."
        except Exception as e:
            print(f"Error parsing Gemini response for {location}: {e}")
            return "Failed to generate alert."

    return "Failed to generate alert after multiple retries."

def main():
    """Main function to orchestrate the entire workflow."""
    print("--- Starting Weather Alert Generation Script ---")

    # Task 1: Get airport list and weather forecasts
    airports = get_large_us_airports()
    weather_alerts_data = []

    if not airports:
        print("No airports found. Exiting.")
        return

    for airport in airports:
        lat, lon = airport['latitude_deg'], airport['longitude_deg']
        iata = airport['iata_code']
        location = airport['municipality']

        print(f"Processing weather for {location} ({iata})...")
        forecast = get_weather_forecast(lat, lon, iata)
        if forecast:
            # Task 2: Generate alert with Gemini and store in list
            alert_text = generate_alert_with_gemini(location, forecast)
            weather_alerts_data.append({
                "airport_iata": iata,
                "location": location,
                "generated_alert": alert_text,
                "original_forecast": forecast
            })
            print(f"Generated alert for {location}.")
        else:
            print(f"Skipping {location} due to failed forecast retrieval.")

    # Task 3: Load generated alerts to a new BigQuery table
    if not weather_alerts_data:
        print("No weather alerts were generated. Exiting.")
        return

    print(f"Loading {len(weather_alerts_data)} alerts to BigQuery table {BQ_TABLE_ID_ALERTS}...")
    try:
        bigquery_client = bigquery.Client()
        table_ref = bigquery_client.dataset(BQ_DATASET_ID).table(BQ_TABLE_ID_ALERTS)
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("airport_iata", "STRING"),
                bigquery.SchemaField("location", "STRING"),
                bigquery.SchemaField("generated_alert", "STRING"),
                bigquery.SchemaField("original_forecast", "STRING"),
            ],
            write_disposition="WRITE_TRUNCATE",
        )
        job = bigquery_client.load_table_from_json(
            weather_alerts_data,
            table_ref,
            job_config=job_config
        )
        job.result()
        print(f"Successfully loaded alerts to {BQ_TABLE_ID_ALERTS}.")
    except Exception as e:
        print(f"Error loading alerts to BigQuery: {e}")

    print("--- Script Finished ---")

if __name__ == "__main__":
    main()
