import dash
from dash import dcc, html
import pandas as pd
import sqlite3
import plotly.express as px

# Database Configuration
DB_NAME = "sales_data.db"

def fetch_data():
    """Fetch sales data from SQLite"""
    with sqlite3.connect(DB_NAME) as conn:
        df = pd.read_sql("SELECT product, SUM(total_revenue) as revenue FROM sales GROUP BY product", conn)
    return df

# Initialize Dash app
app = dash.Dash(__name__)

app.layout = html.Div(children=[
    html.H1("Sales Dashboard", style={"text-align": "center"}),

    dcc.Graph(id="sales_chart")
])

@app.callback(
    dash.Output("sales_chart", "figure"),
    dash.Input("sales_chart", "id")  # Dummy input to trigger updates
)
def update_chart(_):
    df = fetch_data()

    if df.empty:
        return px.bar(title="No Data Available")

    fig = px.bar(df, x="product", y="revenue", title="Total Revenue by Product", color="revenue")
    return fig

# Run the Dash app
if __name__ == "__main__":
    app.run_server(debug=True)