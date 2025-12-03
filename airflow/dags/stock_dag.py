from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.fetch_and_store import fetch_and_store


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="stock_price_pipeline",
    default_args=default_args,
    description="Fetch stock prices from Alpha Vantage and store in Postgres",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@hourly",  # or '@daily'
    catchup=False,
    tags=["stocks", "example"],
) as dag:

    fetch_and_store_task = PythonOperator(
        task_id="fetch_and_store_stock_data",
        python_callable=fetch_and_store,
    )

