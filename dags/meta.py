from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

DAG_ID = "meta"

with DAG(
    DAG_ID,
    default_args={
        "depends_on_past": True,
        "retries": 1,
        "retry_delay": timedelta(seconds=3)
    },
    description="Processing weather data",
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    end_date=datetime(2024, 6, 1),
    catchup=True,
    max_active_runs=1,
    tags=["spark", "submit", "meta"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    create_meta = BashOperator(
        task_id="create_meta",
        bash_command=""" # -> 인스턴스용
            ssh -i ~/.ssh/gcp-joon-key joon@34.47.101.222 \
            "/home/joon/code/WeatherTunes/features/meta/run.sh {{ ds_nodash }} /home/joon/code/WeatherTunes/features/meta/a.py"
        """
    )

    # 📦 BigQuery로 parquet 파일 업로드
    copy_to_bigquery = BashOperator(
        task_id="copy_to_bigquery",
        bash_command="""
        bq load \
        --source_format=PARQUET \
        --autodetect \
        --append \
        weathertunes:meta.meta \
        gs://jacob_weathertunes/meta/dt={{ ds_nodash }}/*.parquet
        """
    )

    start >> create_meta >> copy_to_bigquery >> end
