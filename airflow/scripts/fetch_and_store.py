import os
import logging
from datetime import datetime

import requests
import psycopg2
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def get_db_connection():
    """Create and return a Postgres connection using env vars."""
    conn = psycopg2.connect(
        dbname=os.environ.get("STOCK_DB_NAME"),
        user=os.environ.get("STOCK_DB_USER"),
        password=os.environ.get("STOCK_DB_PASSWORD"),
        host=os.environ.get("STOCK_DB_HOST"),
        port=os.environ.get("STOCK_DB_PORT", 5432),
    )
    return conn

def fetch_stock_data():
    """
    Fetch stock data from Alpha Vantage (intraday 5min).
    """
    api_key = os.environ.get("ALPHA_VANTAGE_API_KEY")
    symbol = os.environ.get("STOCK_SYMBOL", "IBM")

    if not api_key:
        raise ValueError("ALPHA_VANTAGE_API_KEY is not set")

    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": symbol,
        "interval": "5min",
        "apikey": api_key
    }

    logger.info("Requesting data from Alpha Vantage for %s", symbol)

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error("Error while calling Alpha Vantage API: %s", e)
        raise

    data = response.json()

    if "Time Series (5min)" not in data:
        logger.error("Unexpected API response: %s", data)
        raise ValueError("Missing 'Time Series (5min)' in API response")

    timeseries = data["Time Series (5min)"]
    rows = []

    for timestamp_str, values in timeseries.items():
        try:
            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
            open_price = float(values["1. open"])
            high = float(values["2. high"])
            low = float(values["3. low"])
            close = float(values["4. close"])
            volume = int(float(values["5. volume"]))
        except (KeyError, ValueError) as e:
            logger.warning("Skipping record due to parse error: %s", e)
            continue

        rows.append(
            (
                symbol,
                close,
                open_price,
                high,
                low,
                volume,
                timestamp,
            )
        )

    logger.info("Parsed %d rows of stock data", len(rows))
    return rows

def upsert_stock_data(rows):
    """Insert rows into stock_prices table."""
    if not rows:
        logger.info("No rows to insert")
        return

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        insert_query = """
            INSERT INTO stock_prices
                (symbol, price, open, high, low, volume, timestamp)
            VALUES %s
        """

        execute_values(cur, insert_query, rows)
        conn.commit()
        logger.info("Inserted %d rows into stock_prices", len(rows))

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error("Error inserting into DB: %s", e)
        raise
    finally:
        if conn:
            conn.close()

def fetch_and_store():
    """Main function for Airflow task."""
    try:
        rows = fetch_stock_data()
        upsert_stock_data(rows)
    except Exception as e:
        logger.error("Pipeline failed: %s", e)
        raise

