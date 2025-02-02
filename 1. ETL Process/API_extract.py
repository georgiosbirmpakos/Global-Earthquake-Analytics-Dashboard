import pandas as pd
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from Postgress_DB_Connector import Postgress_DB_Connector

# Function to get the start and end date of each month
def get_month_date_range(year, month):
    start_date = datetime(year, month, 1)
    if month == 12:
        end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
    else:
        end_date = datetime(year, month + 1, 1) - timedelta(days=1)
    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

# Function to fetch data for a given month
def fetch_data_for_month(start_date, end_date):
    base_url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    params = {
        "format": "geojson",
        "starttime": start_date,
        "endtime": end_date,
        "limit": 20000
    }

    try:
        response = requests.get(base_url, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            return data.get('features', [])
        else:
            print(f"Error {response.status_code} for {start_date} - {end_date}")
            return []
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return []

# Transform API event data into a list of dictionaries for batch insertion
def transform_event(event):
    properties = event.get('properties', {})
    geometry = event.get('geometry', {}).get('coordinates', [None, None, None])

    return {
        'time': datetime.utcfromtimestamp(properties.get('time', 0) / 1000) if properties.get('time') else None,
        'latitude': geometry[1],
        'longitude': geometry[0],
        'depth': geometry[2],
        'magnitude': properties.get('mag'),
        'place': properties.get('place'),
        'type': properties.get('type'),
        'status': properties.get('status'),
        'tsunami': properties.get('tsunami'),
        'sig': properties.get('sig')
    }

# Fetch and store data concurrently
def main():
    year = 2024
    db = Postgress_DB_Connector()  # Initialize DB Manager

    all_events = []  # Collect all events for batch insert

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        for month in range(1, 13):
            start_date, end_date = get_month_date_range(year, month)
            futures.append(executor.submit(fetch_data_for_month, start_date, end_date))

        for future in futures:
            events = future.result()
            transformed_events = [transform_event(event) for event in events]
            all_events.extend(transformed_events)

    # Insert all events at once (batch insert for better performance)
    if all_events:
        df = pd.DataFrame(all_events)
        db.insert_data(df, 'earthquakes')

    print("Data inserted into PostgreSQL successfully!")

# Run the script
if __name__ == "__main__":
    main()