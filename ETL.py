# %%
'''Basic **ETL (Extract, Transform, Load)** process:

1. **Extract:**  
   The script fetches earthquake data from the **USGS Earthquake API** using the `requests` library.
     The `fetch_data_for_month` function handles data extraction for specific date ranges.

2. **Transform:**  
   The data undergoes minimal transformation in the `insert_data` function.
     It extracts specific fields like time, latitude, longitude, depth, magnitude, etc., and converts the timestamp to a datetime format.

3. **Load:**  
   The transformed data is inserted into a PostgreSQL database using SQL `INSERT` statements. 
   This happens within the `insert_data` function, and data is committed after each insert.
'''

import os
import requests
from datetime import datetime, timedelta
import psycopg2
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor

# Load environment variables from .env file
load_dotenv()

# PostgreSQL connection details
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Connect to PostgreSQL
conn = psycopg2.connect(
    host=DB_HOST,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)
cur = conn.cursor()

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

# Insert data into PostgreSQL
def insert_data(event):
    properties = event.get('properties', {})
    geometry = event.get('geometry', {}).get('coordinates', [None, None, None])

    time = datetime.utcfromtimestamp(properties.get('time', 0) / 1000) if properties.get('time') else None
    latitude = geometry[1]
    longitude = geometry[0]
    depth = geometry[2]
    magnitude = properties.get('mag')
    place = properties.get('place')
    event_type = properties.get('type')
    status = properties.get('status')
    tsunami = properties.get('tsunami')
    sig = properties.get('sig')

    cur.execute("""
        INSERT INTO earthquakes (time, latitude, longitude, depth, magnitude, place, type, status, tsunami, sig)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (time, latitude, longitude, depth, magnitude, place, event_type, status, tsunami, sig))

# Fetch and store data concurrently
def main():
    year = 2024
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        for month in range(1, 13):
            start_date, end_date = get_month_date_range(year, month)
            futures.append(executor.submit(fetch_data_for_month, start_date, end_date))

        for future in futures:
            events = future.result()
            for event in events:
                insert_data(event)
                conn.commit()  # Commit after each insert (or batch if preferred)

    print("Data inserted into PostgreSQL successfully!")

# Run the script
if __name__ == "__main__":
    main()

# Close the database connection
cur.close()
conn.close()



