from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

DAG_ID = "data_songs"

with DAG(
    DAG_ID,
    default_args={
        "depends_on_past": True,
        "retries": 1,
        "retry_delay": timedelta(seconds=3)
    },
    description="Fetching raw weather data",
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    end_date=datetime(2025, 4, 2),
    catchup=True,
    max_active_runs=7,
    concurrency=10,
    tags=["spark", "submit", "weather"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    process_songs_data = BashOperator(
        task_id="process_songs_data",
        bash_command="""
            ssh -i ~/.ssh/gcp-joon-key joon@34.47.90.224 \
            "/home/joon/code/WeatherTunes/features/data_songs/run.sh {{ ds }} /home/joon/code/WeatherTunes/features/data_songs/a.py"
        """
    )

    start >> process_songs_data >> end
