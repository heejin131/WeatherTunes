from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import io
import pandas as pd
from google.cloud import storage

# —————————————————————————————————————————————
# 1) 분류 함수 정의
# —————————————————————————————————————————————
def classify_weather(SD_DAY: float, SD_HR3: float,
                     RN: float, RN_DAY: float,
                     CA_TOT: int) -> int:
    # 눈(3) > 비(2) > 흐림(1) > 맑음(0)
    if SD_DAY > 0 or SD_HR3 > 0:
        return 3
    elif RN > 0 or RN_DAY > 0:
        return 2
    elif CA_TOT >= 5:
        return 1
    else:
        return 0

def classify_temp(TA: float) -> int:
    # 0도 이하:0, 0–10:1, 10–15:2, 15–20:3, 20–25:4, 25이상:5
    if TA <= 0:
        return 0
    elif TA <= 10:
        return 1
    elif TA <= 15:
        return 2
    elif TA <= 20:
        return 3
    elif TA <= 25:
        return 4
    else:
        return 5

# —————————————————————————————————————————————
# 2) 전처리 함수 수정
# —————————————————————————————————————————————
def clean_weather_data(**context):
    ds = context['ds']  # execution date as YYYY-MM-DD
    raw_path = f"raw/weather_raw/dt={ds}/weather.csv"
    out_path = f"data/weather_daily/dt={ds}/weather_daily.parquet"

    client = storage.Client()
    bucket = client.bucket("YOUR_GCS_BUCKET")
    content = bucket.blob(raw_path).download_as_text()

    # CSV 읽기
    df = pd.read_csv(io.StringIO(content))

    # 필요한 컬럼만 선택 & 결측치 처리
    df = df[['TA', 'SD_DAY', 'SD_HR3', 'RN', 'RN_DAY', 'CA_TOT']].fillna(0)

    # 날짜 컬럼 추가
    df['date'] = pd.to_datetime(ds)

    # CA_TOT 안전 변환 (float → int)
    df['CA_TOT'] = df['CA_TOT'].astype(float).fillna(0).astype(int)

    # 날씨 코드, 기온 코드 적용
    df['weather_code'] = df.apply(
        lambda r: classify_weather(
            r['SD_DAY'], r['SD_HR3'],
            r['RN'], r['RN_DAY'],
            r['CA_TOT']
        ),
        axis=1
    )
    df['temp_code'] = df['TA'].apply(classify_temp)

    # Parquet으로 저장
    out_blob = bucket.blob(out_path)
    out_blob.upload_from_string(
        df.to_parquet(index=False),
        content_type='application/octet-stream'
    )

# —————————————————————————————————————————————
# 3) DAG 정의
# —————————————————————————————————————————————
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='data_weather',
    description='Clean raw weather data into daily summarized parquet',
    default_args=default_args,
    schedule_interval='@daily',       # 매일 한 번 실행
    start_date=days_ago(1),
    catchup=False,
    tags=['weathertunes', 'data']
) as dag:

    clean_weather = PythonOperator(
        task_id='clean_weather_data',
        python_callable=clean_weather_data,
        provide_context=True,
    )

    clean_weather