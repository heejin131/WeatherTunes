from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

DAG_ID = "data_weather"

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
    end_date=datetime(2025, 4, 2),
    catchup=True,
    max_active_runs=1,
    tags=["spark", "submit", "weather"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    process_weather_data = BashOperator(
        task_id="process_weather_data",
        bash_command="""
            ssh -i ~/.ssh/gcp-joon-key joon@34.22.105.106 \
            "/home/joon/code/WeatherTunes/features/data_weather/run.sh {{ ds_nodash }} /home/joon/code/WeatherTunes/features/data_weather/a.py"
        """
    )

    start >> process_weather_data >> end
