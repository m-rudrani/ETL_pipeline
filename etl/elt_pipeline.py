import pandas as pd
import sqlite3
from sqlalchemy import create_engine
import schedule
import time
import logging

# Database Configuration
DB_NAME = "sales_data.db"
TABLE_NAME = "sales"
engine = create_engine(f"sqlite:///{DB_NAME}")

# Logging Configuration
logging.basicConfig(filename="etl_pipeline.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def create_table():
    """Create the sales table if it doesn't exist."""
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
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
    """Extract data from CSV file"""
    try:
        df = pd.read_csv("sales_data.csv")
        logging.info(f"Extracted {len(df)} records from CSV file.")
        return df
    except Exception as e:
        logging.error(f"Failed to extract data: {e}")
        return pd.DataFrame()  # Return empty DataFrame if extraction fails


def transform(df):
    """Clean and transform data"""
    if df.empty:
        logging.warning("No data to transform.")
        return df

    df.dropna(inplace=True)  # Remove rows with missing values
    df["sale_date"] = pd.to_datetime(df["sale_date"], errors="coerce").astype(str)  # Convert dates safely
    df["total_revenue"] = df["quantity"] * df["price"]  # Calculate revenue

    logging.info("Transformed data successfully.")
    return df


def load(df):
    """Load data into SQLite database while preventing duplicates"""
    if df.empty:
        logging.warning("No data to load.")
        return

    with sqlite3.connect(DB_NAME) as conn:
        existing_data = pd.read_sql(f"SELECT order_id FROM {TABLE_NAME}", conn)

        # Filter out already existing records
        new_data = df[~df["order_id"].isin(existing_data["order_id"])]
        
        if not new_data.empty:
            new_data.to_sql(TABLE_NAME, con=conn, if_exists="append", index=False)
            logging.info(f"Loaded {len(new_data)} new records into the database.")
        else:
            logging.info("No new records to insert.")


def etl_job():
    """Run ETL Pipeline"""
    logging.info("Starting ETL Process...")
    create_table()  # Ensure table exists
    df = extract()
    df = transform(df)
    load(df)
    logging.info("ETL Process Completed!")


# Schedule ETL to run daily at 10 AM
schedule.every().day.at("19:29").do(etl_job)

# Run Scheduler
if __name__ == "__main__":
    create_table()  # Ensure table exists before scheduling

    while True:
        schedule.run_pending()
        time.sleep(1)  # Check every minute
