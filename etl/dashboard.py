import pandas as pd
import sqlite3
import matplotlib.pyplot as plt

DB_NAME = "sales_data.db"

def fetch_data():
    """Fetch sales data from SQLite"""
    with sqlite3.connect(DB_NAME) as conn:
        df = pd.read_sql("SELECT product, SUM(total_revenue) as revenue FROM sales GROUP BY product", conn)
    return df

def plot_data():
    """Visualize sales revenue per product"""
    df = fetch_data()
    
    if df.empty:
        print("No data found in database.")
        return

    # Plot the data
    plt.figure(figsize=(10, 5))
    plt.bar(df["product"], df["revenue"], color="skyblue")
    plt.xlabel("Product")
    plt.ylabel("Total Revenue")
    plt.title("Total Revenue by Product")
    plt.xticks(rotation=45)
    plt.show()

if __name__ == "__main__":
    plot_data()
