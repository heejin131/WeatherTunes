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
    dag_id="data_songs_local",
    default_args=default_args,
    start_date=datetime(2023, 1, 2),
    end_date=datetime(2023, 1, 3),
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=1,
    tags=["weathertunes", "audio", "local"],
) as dag:

    start = EmptyOperator(task_id="start")

    run_audio_feature_job = BashOperator(
    task_id="extract_audio_features_local",
    bash_command="bash /home/wsl/code/WeatherTunes/features/data_songs/run.sh {{ ds_nodash }} features/data_songs/a.py",
    cmd_timeout=1800,
    )
    

    end = EmptyOperator(task_id="end")

    start >> run_audio_feature_job >> end
