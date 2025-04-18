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
    dag_id="data_songs",
    default_args=default_args,
    start_date=datetime(2023, 1, 2),
    end_date=datetime(2025, 4, 2),
    schedule_interval="@daily",
    catchup=True,
    tags=["weathertunes", "audio"],
    max_active_runs=1, 
) as dag:

    start = EmptyOperator(task_id="start")

    process_songs_data = BashOperator(
        task_id="process_songs_data",
        bash_command="""
            ssh -i ~/.ssh/id_rsa wsl@34.64.195.187 \
            "/home/wsl/code/WeatherTunes/features/data_songs/run.sh {{ ds }} /home/wsl/code/WeatherTunes/features/data_songs/b.py"
        """
    )

    extract_audio_features = BashOperator(
        task_id="extract_audio_features",
        bash_command="""
            ssh -i ~/id_rsa wsl@34.64.195.187 \
            "/home/wsl/code/WeatherTunes/features/data_songs/run.sh {{ ds }} /home/wsl/code/WeatherTunes/features/data_songs/a.py"
        """
    )

    end = EmptyOperator(task_id="end")

    start >> process_songs_data  >> extract_audio_features >> end                                                                       
