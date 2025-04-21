from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import os


DAG_ID = "data_weathers"

with DAG(
    DAG_ID,
    default_args = {
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=5),
        },
    description='Clean raw weather data into daily summarized parquet via feature script',
    schedule='@daily',
    start_date=datetime(2023, 1, 1),
    end_date=datetime(2025, 4, 2),
    catchup=True,
    max_active_runs=7,
    concurrency=10,
    tags=['weathertunes', 'weather', 'data'],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    clean_weather_data = BashOperator(
        task_id="process_songs_data",
        bash_command="""
             ssh -i ~/.ssh/WMINHYUK_KEY SEOMINHYUK@34.64.239.242 \
            "/home/SEOMINHYUK/code/airflow/dags/WeatherTunes/features/data_weather/run.sh {{ ds }} /home/SEOMINHYUK/code/airflow/dags/WeatherTunes/features/data_weather/a.py"
        """
    )
    start >> clean_weather_data >> end
