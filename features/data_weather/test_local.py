#!/usr/bin/env python3
import io
import glob
import pandas as pd
from datetime import datetime

def classify_weather(SD_DAY, SD_HR3, RN, RN_DAY, CA_TOT) -> int:
    """
    날씨 구분: 눈(3) > 비(2) > 흐림(1) > 맑음(0)
    결측치는 0으로 처리되었음.
    """
    if SD_DAY > 0 or SD_HR3 > 0:
        return 3
    elif RN > 0 or RN_DAY > 0:
        return 2
    elif CA_TOT >= 5:
        return 1
    else:
        return 0

def classify_temp(TA) -> int:
    """
    기온 구분 코드:
     0도 이하:0, 0–10:1, 10–15:2, 15–20:3, 20–25:4, 25 이상:5
    TA는 항상 유효하므로 별도 결측 처리 없음.
    """
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

def test_file(path):
    # 이 함수는 date, weather_code, temp_code만 출력하도록 수정됨
    raw = open(path).read()

    # 1) 헤더 없이 whitespace + comment 처리
    df0 = pd.read_csv(
        io.StringIO(raw),
        sep=r"\s+",
        comment="#",
        header=None,
        engine="python",
    )

    # 2) 필요한 필드 위치(Index)에서 추출
    df = pd.DataFrame({
        'TA':      df0.iloc[:, 11].astype(float),
        'RN':      df0.iloc[:, 15].astype(float),
        'RN_DAY':  df0.iloc[:, 16].astype(float),
        'SD_HR3':  df0.iloc[:, 19].astype(float),
        'SD_DAY':  df0.iloc[:, 20].astype(float),
        'CA_TOT':  df0.iloc[:, 25].astype(float),
    })

    # 3) RN, RN_DAY, SD_HR3, SD_DAY, CA_TOT 컬럼만 sentinel 값을 0으로 대체
    df[['RN','RN_DAY','SD_HR3','SD_DAY','CA_TOT']] = \
        df[['RN','RN_DAY','SD_HR3','SD_DAY','CA_TOT']].replace({-9: 0, -99: 0, -999: 0})

    # 4) 날짜 컬럼 생성 및 포맷 (YYYYMMDD)
    df['date'] = datetime.strptime("2025-04-17", "%Y-%m-%d").strftime("%Y%m%d")

    # 5) 분류 적용
    df['weather_code'] = df.apply(
        lambda r: classify_weather(
            r['SD_DAY'], r['SD_HR3'],
            r['RN'], r['RN_DAY'],
            r['CA_TOT']
        ),
        axis=1
    )
    df['temp_code'] = df['TA'].apply(classify_temp)

    # 6) 필요한 컬럼만 선택하여 출력
    print(df[['date', 'weather_code', 'temp_code']].head().to_string(index=False))

if __name__ == "__main__":
    for path in glob.glob("features/test/test_weather_txt/*.txt"):
        print(f"\n=== {path} ===")
        test_file(path)