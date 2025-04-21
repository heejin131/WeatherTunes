from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
import os

# 💡 scripts 디렉토리 경로 추가
sys.path.append(os.path.join(os.path.dirname(__file__), "../script"))
from app import generate_meta_profile

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 4, 13),
    "end_date": datetime(2025, 4, 16),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dag_generate_meta_profile",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    catchup=True,
    tags=["meta", "profile"]
) as dag:

    start = EmptyOperator(task_id="start")

    # 🧪 Spark 함수로 meta profile 생성 후 GCS 저장

    generate = BashOperator(
        task_id="generate_meta_profile",
        bash_command="""
        spark-submit \
        /home/gmlwls5168/airflow/script/app.py {{ ds_nodash }}
        """
    )

    # 📦 BigQuery로 parquet 파일 업로드
    copy_to_bigquery = BashOperator(
        task_id="copy_to_bigquery",
        bash_command="""
        bq load \
        --source_format=PARQUET \
        --autodetect \
        --replace \
        praxis-zoo-455400-d2:meta_dataset.meta_profile \
        gs://nijin-bucket/meta/meta_profile/{{ ds_nodash }}/weather_main=0/temp_band=1/*.parquet
        """
    )

    end = EmptyOperator(task_id="end")

    start >> generate >> copy_to_bigquery >> end
