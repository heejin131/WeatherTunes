from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import pendulum

DAG_ID = "recommendations"

with DAG(
    DAG_ID,
    default_args={
        "depends_on_past": True,
        "retries": 1,
        "retry_delay": timedelta(seconds=3)
    },
    description="Notify recommended songs by Discord",
    schedule="* 8 * * *",
    start_date=pendulum.datetime(2025, 4, 1, tz="Asia/Seoul"),
    catchup=True,
    max_active_runs=1,
    tags=["spark", "submit", "notification"],
) as dag:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    load_recommended_songs = BashOperator(
        task_id="load_recommended_songs",
        bash_command="""
            ssh -i ~/.ssh/gcp-joon-key joon@34.47.101.222 \
            "/home/joon/code/WeatherTunes/features/recommedations/run.sh {{ ds_nodash }} /home/joon/code/WeatherTunes/features/recommedations/a.py"
        """
    )
    
    send_notification = BashOperator(
        task_id="send_notification",
        bash_command="""
            ssh -i ~/.ssh/gcp-joon-key joon@34.47.101.222 \
            "/home/joon/code/WeatherTunes/features/recommedations/run.sh {{ ds }} /home/joon/code/WeatherTunes/features/recommedations/discord.py"
        """
    )
    
    start >> load_recommended_songs >> send_notification >> end
