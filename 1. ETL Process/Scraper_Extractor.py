import requests
from bs4 import BeautifulSoup
import pandas as pd
from Postgress_DB_Connector import Postgress_DB_Connector 

class Scraper_Extractor:
    def __init__(self, url):
        self.url = url
        self.db = Postgress_DB_Connector()  # Initialize DB manager

    def extract_data(self):
        """Extract earthquake data from the website."""
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(self.url, headers=headers)

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            table = soup.find('table', {'class': 'sortable'})

            # Extract headers
            table_headers = [th.get_text(strip=True) for th in table.find('thead').find_all('th')]

            # Extract data rows
            rows = []
            for row in table.find('tbody').find_all('tr'):
                cols = [td.get_text(strip=True) for td in row.find_all('td')]
                if len(cols) == len(table_headers):
                    rows.append(cols)

            # Create DataFrame
            df = pd.DataFrame(rows, columns=table_headers)
            return df
        else:
            print(f"Failed to retrieve the page. Status code: {response.status_code}")
            return pd.DataFrame()

    def transform_data(self, df):
        """Transform the extracted data to match the database schema."""
        df.rename(columns={
            'Origin Time(GMT)': 'time',
            'Latitude(°N)': 'latitude',
            'Longitude(°E)': 'longitude',
            'Depth(km)': 'depth',
            'Mag.': 'magnitude',
            'Epicentral Location': 'place',
            'Sol.Type': 'type'
        }, inplace=True)

        # Add missing columns with default values
        df['status'] = 'reviewed'
        df['tsunami'] = 0
        df['sig'] = 0

        # Rearrange columns to match database table structure
        df = df[['time', 'latitude', 'longitude', 'depth', 'magnitude', 'place', 'type', 'status', 'tsunami', 'sig']]
        return df

    def load_data(self, df, table_name='earthquakes'):
        """Load the transformed data into PostgreSQL."""
        if not df.empty:
            self.db.insert_data(df, table_name)
            print("Data successfully loaded into PostgreSQL.")
        else:
            print("No data to load.")

    def verify_insertion(self):
        """Fetch and display the latest records to verify the insertion."""
        result_df = self.db.fetch_data("SELECT * FROM earthquakes ORDER BY id DESC LIMIT 5;")
        return result_df

    def run(self):
        """Main ETL pipeline."""
        print("Starting ETL process...")

        # Extract
        df = self.extract_data()
        print(f"Extracted {len(df)} records.")

        # Transform
        df = self.transform_data(df)
        print(f"Transformed data with {df.shape[1]} columns.")

        # Load
        self.load_data(df)

        # Verify
        result_df = self.verify_insertion()
        print("ETL process completed successfully.")

# Main execution
if __name__ == "__main__":
    url = 'http://www.geophysics.geol.uoa.gr/stations/maps/recent.html'
    pipeline = Scraper_Extractor(url)
    pipeline.run()
