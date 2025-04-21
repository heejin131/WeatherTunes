from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from scripts.spotify_gcs_job import run_spotify_job

default_args = {
    "owner": "spotify-bot",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="spotify_daily_to_gcs",
    default_args=default_args,
    description="🇰🇷 Spotify Daily Chart → GCS 저장",
    schedule_interval="0 10 * * *",  # 매일 UTC 10시 (KST 19시)
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2025, 1, 1),
    max_active_runs=1,
    catchup=True,
    concurrency=2,
    tags=["spotify", "gcs", "selenium"],
) as dag:

    start = EmptyOperator(task_id="start")

    download_and_upload_chart = PythonOperator(
        task_id="download_and_upload_chart",
        python_callable=run_spotify_job,
        op_kwargs={"ds": "{{ ds }}"},
    )

    end = EmptyOperator(task_id="end")

    start >> download_and_upload_chart >> end
