from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="data_song200_clean",
    schedule_interval="10 10 * * *",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2025, 4, 3),
    max_active_runs=3,
    catchup=True,
    tags=["spark", "spotify","song","cleanse", "gcs"],
) as dag:

    start = EmptyOperator(task_id="start")

    run_spark = BashOperator(
        task_id="run_spark_job",
        bash_command="/home/jacob8753/airflow2/dags/scripts/data_song200_run.sh {{ ds }}",
    )

    end = EmptyOperator(task_id="end")

    start >> run_spark >> end
