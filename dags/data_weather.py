from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='data_weather',
    description='Clean raw weather data into daily summarized parquet via feature script',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['weathertunes', 'data'],
) as dag:

    clean_weather = BashOperator(
        task_id='clean_weather_data',
        bash_command=(
            # Airflow variable로 프로젝트 루트 지정하거나 절대경로로 작성
            "bash {{ var.value.project_root }}/features/data_weather/run.sh {{ ds }}"
        ),
        env={'GCS_BUCKET': os.environ.get('GCS_BUCKET', '')},
    )

    clean_weather