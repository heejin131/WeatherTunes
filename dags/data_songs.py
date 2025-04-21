from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="extract_audio_features_local",
    default_args=default_args,
    start_date=datetime(2023, 1, 2),
    end_date=datetime(2025, 4, 2),
    schedule_interval="@daily",
    catchup=True,
    tags=["weathertunes", "audio"],
    max_active_runs=1,
    max_active_tasks=1,
) as dag:

    start = EmptyOperator(task_id="start")

    extract_audio_features = BashOperator(
        task_id="extract_audio_features",
        bash_command="""
            cd /home/wsl/code/WeatherTunes/features/data_songs && \
            /home/wsl/.pyenv/versions/weathertunes-3.12/bin/python a.py {{ ds }}
        """
    )

    end = EmptyOperator(task_id="end")

    start >> extract_audio_features >> end
