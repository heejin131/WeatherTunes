#!/usr/bin/env python3
import argparse
import io
import os
import pandas as pd
from google.cloud import storage
from datetime import datetime

def classify_weather(SD_DAY, SD_HR3, RN, RN_DAY, CA_TOT) -> int:
    # 눈(3) > 비(2) > 흐림(1) > 맑음(0)
    if SD_DAY > 0 or SD_HR3 > 0:
        return 3
    elif RN > 0 or RN_DAY > 0:
        return 2
    elif CA_TOT >= 5:
        return 1
    else:
        return 0

def classify_temp(TA) -> int:
    # 기온 구분 (0℃ 이하:0, 0–10:1, …, >25:5)
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

def clean_weather_date(ds: str, bucket_name: str):
    # 날짜 포맷 검사 및 Parquet용 문자열 생성
    try:
        date_obj = datetime.strptime(ds, "%Y-%m-%d")
    except ValueError:
        raise SystemExit(f"ERROR: --date must be YYYY-MM-DD, got {ds}")
    date_str = date_obj.strftime("%Y%m%d")

    raw_path = f"raw/weather_raw/dt={ds}/weather.txt"
    out_path = f"data/weather_daily/dt={ds}/weather_daily.parquet"

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    content = bucket.blob(raw_path).download_as_text()

    # 1) TXT 읽기: 주석 제거, 공백/탭 분리
    df0 = pd.read_csv(
        io.StringIO(content),
        sep=r"\s+",
        comment="#",
        header=None,
        engine="python"
    )

    # 2) 필요한 필드만 뽑아 DataFrame 생성
    df = pd.DataFrame({
        'TA':      df0.iloc[:, 11].astype(float),
        'RN':      df0.iloc[:, 15].astype(float),
        'RN_DAY':  df0.iloc[:, 16].astype(float),
        'SD_HR3':  df0.iloc[:, 19].astype(float),
        'SD_DAY':  df0.iloc[:, 20].astype(float),
        'CA_TOT':  df0.iloc[:, 25].astype(float),
    })

    # 3) sentinel 값(-9, -99, -999) → 0으로 (“관측 없음” 처리)
    df[['RN','RN_DAY','SD_HR3','SD_DAY','CA_TOT']] = \
        df[['RN','RN_DAY','SD_HR3','SD_DAY','CA_TOT']].replace({-9: 0, -99: 0, -999: 0})

    # 4) 분류 적용
    df['weather_code'] = df.apply(
        lambda r: classify_weather(
            r['SD_DAY'], r['SD_HR3'],
            r['RN'], r['RN_DAY'],
            r['CA_TOT']
        ),
        axis=1
    )
    df['temp_code'] = df['TA'].apply(classify_temp)

    # 5) date 컬럼 추가 (YYYYMMDD)
    df['date'] = date_str

    # 6) Parquet에는 date, weather_code, temp_code만 저장
    result = df[['date', 'weather_code', 'temp_code']]

    out_blob = bucket.blob(out_path)
    out_blob.upload_from_string(
        result.to_parquet(index=False),
        content_type='application/octet-stream'
    )
    print(f"[OK] cleaned weather for {ds} → {out_path}")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--date",   required=True,
                   help="processing date in YYYY-MM-DD")
    p.add_argument("--bucket", default=os.environ.get("GCS_BUCKET"),
                   help="GCS bucket name (env GCS_BUCKET)")
    args = p.parse_args()

    if not args.bucket:
        raise SystemExit("ERROR: must set --bucket or $GCS_BUCKET")

    clean_weather_date(args.date, args.bucket)