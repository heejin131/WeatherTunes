from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

DAG_ID = "raw_weather"

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
    max_active_runs=3,
    tags=["spark", "submit", "weather"],
) as dag:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    fetch_today_weather = BashOperator(
        task_id="fetch_today_weather",
        bash_command="""
            ssh -i ~/.ssh/gcp-joon-key joon@34.22.91.104 \
            "/home/joon/code/WeatherTunes/features/raw_weather/run.sh {{ ds_nodash }} /home/joon/code/WeatherTunes/features/raw_weather/a.py"
        """
    )
    
    start >> fetch_today_weather >> end
