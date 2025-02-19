import pandas as pd
import sqlite3
import requests
from sqlalchemy import create_engine
import schedule
import logging
import time

# SQLite Database
DB_NAME = "sales.db"
engine = create_engine(f"sqlite:///{DB_NAME}")

# FakeStore API Endpoint (Returns fake e-commerce product data)
API_URL = "https://fakestoreapi.com/products"

# Logging Configuration
logging.basicConfig(filename="etl_api.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def create_table():
    """Create the sales table if it doesn't exist."""
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sales (
                order_id INTEGER PRIMARY KEY,
                product TEXT,
                quantity INTEGER,
                price REAL,
                total_revenue REAL,
                sale_date TEXT
            )
        """)
        conn.commit()
    logging.info("Checked/Created sales table in database.")


def extract():
    """Extract data from the FakeStore API"""
    try:
        response = requests.get(API_URL)
        response.raise_for_status()  # Raise exception for HTTP errors (4xx, 5xx)

        data = response.json()
        df = pd.DataFrame(data)
        logging.info(f"Extracted {len(df)} records from API.")
        return df

    except requests.RequestException as e:
        logging.error(f"Failed to extract data: {e}")
        print("Failed to fetch data from API")
        return pd.DataFrame()


def transform(df):
    """Clean and transform data"""
    if df.empty:
        logging.warning("No data to transform.")
        return df

    df = df.rename(columns={"id": "order_id", "title": "product", "price": "price"})
    df["quantity"] = 1  # Assume quantity = 1 for simplicity
    df["total_revenue"] = df["quantity"] * df["price"]  # Calculate total revenue
    df["sale_date"] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')  # Add timestamp

    logging.info("Transformed data successfully.")
    return df[["order_id", "product", "quantity", "price", "total_revenue", "sale_date"]]


def load(df):
    """Load data into SQLite database"""
    if not df.empty:
        with sqlite3.connect(DB_NAME) as conn:
            df.to_sql("sales", con=conn, if_exists="append", index=False)
        logging.info(f"Loaded {len(df)} new records into the database.")
        print("Data loaded successfully!")
    else:
        logging.warning("No data to load.")
        print("No data to load.")


def etl_job():
    """Run ETL Pipeline"""
    logging.info("Starting ETL Process...")
    print("Starting ETL Process...")

    create_table()  # Ensure table exists before inserting data
    df = extract()
    df = transform(df)
    load(df)

    logging.info("ETL Process Completed!")
    print("ETL Process Completed!")


# Schedule ETL to run every 5 minutes
schedule.every(5).minutes.do(etl_job)

# Run Scheduler
if __name__ == "__main__":
    etl_job()  # Run once before scheduling
    while True:
        schedule.run_pending()
        time.sleep(1)
