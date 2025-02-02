# %%
import os
from dotenv import load_dotenv
import psycopg2
import pandas as pd

# Load environment variables
load_dotenv()

class Postgress_DB_Connector:
    def __init__(self):
        # Load DB credentials from environment variables
        self.DB_HOST = os.getenv("DB_HOST")
        self.DB_NAME = os.getenv("DB_NAME")
        self.DB_USER = os.getenv("DB_USER")
        self.DB_PASSWORD = os.getenv("DB_PASSWORD")

    def _connect(self):
        """Private method to open the database connection."""
        return psycopg2.connect(
            host=self.DB_HOST,
            database=self.DB_NAME,
            user=self.DB_USER,
            password=self.DB_PASSWORD
        )

    def insert_data(self, df, table_name):
        """Insert DataFrame records into the specified table."""
        columns = df.columns.tolist()
        placeholders = ', '.join(['%s'] * len(columns))
        column_names = ', '.join(columns)

        insert_query = f"""
            INSERT INTO {table_name} ({column_names})
            VALUES ({placeholders})
        """

        # Open connection only when needed
        conn = self._connect()
        cur = conn.cursor()

        try:
            for index, row in df.iterrows():
                cur.execute(insert_query, tuple(row))
            conn.commit()
            print(f"Data inserted successfully into '{table_name}'.")
        except Exception as e:
            conn.rollback()
            print(f"Error during insertion: {e}")
        finally:
            cur.close()
            conn.close()  # Ensure the connection is always closed

    def fetch_data(self, query):
        """Fetch data from the database and return as a DataFrame."""
        conn = self._connect()
        cur = conn.cursor()

        try:
            cur.execute(query)
            results = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            return pd.DataFrame(results, columns=columns)
        except Exception as e:
            print(f"Error during fetch: {e}")
            return pd.DataFrame()  # Return an empty DataFrame on error
        finally:
            cur.close()
            conn.close()  # Ensure the connection is always closed



