import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

def create_weather_code(sd_day, sd_hr3, rn, rn_day, ca_tot) -> int:
    if (sd_day is not None and sd_day > 0) or (sd_hr3 is not None and sd_hr3 > 0):
        return 3  # 눈
    elif (rn is not None and rn > 0) or (rn_day is not None and rn_day > 0):
        return 2  # 비
    elif ca_tot is not None and ca_tot >= 5:
        return 1  # 흐림
    return 0      # 맑음

def create_temp_code(ta) -> int:
    if ta <= 0:
        return 0
    elif ta <= 10:
        return 1
    elif ta <= 15:
        return 2
    elif ta <= 20:
        return 3
    elif ta <= 25:
        return 4
    return 5

def convert_to_parquet(raw_data_path: str, save_path: str, dt: str):
    # 필요한 컬럼들
    cols = ["SD_DAY", "SD_HR3", "RN", "RN_DAY", "CA_TOT", "TA"]
    
    # Spark 세션 시작
    spark = SparkSession.builder.appName(f"process_weather_data_{dt}") \
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
            .getOrCreate()
    
    # csv에서 데이터 로드
    df = spark.read.option("header", "true").csv(raw_data_path)
    
    # 필요한 컬럼들의 평균치 계산
    df_avg = df.select(cols).agg(
        *[avg(c).alias(c) for c in cols]
    ).first()

    # 하루 평균 정보를 통해 날씨 코드 계산
    weather_code = create_weather_code(
        df_avg["SD_DAY"], df_avg["SD_HR3"], 
        df_avg["RN"], df_avg["RN_DAY"], df_avg["CA_TOT"]
    )
    # 하루 평균 기온을 통해 기온 코드 계산
    temp_code = create_temp_code(df_avg["TA"])

    # 날짜, 날씨 코드, 기온 코드만을 가진 데이터프레임 생성
    rdf = spark.createDataFrame(
        [(dt, weather_code, temp_code)],
        ["dt", "weather_code", "temp_code"]
    )
    
    # 저장
    rdf.write.mode("overwrite").partitionBy("dt").parquet(save_path)

    print(f"✅ 저장 완료: {save_path} (dt={dt})")

if __name__ == "__main__":
    ds_nodash = sys.argv[1]
    raw_data_path = f"gs://jacob_weathertunes/raw/weather_raw/weather_raw-{ds_nodash}.csv"
    save_path = f"gs://jacob_weathertunes/data/weather_data/"

    convert_to_parquet(raw_data_path, save_path, ds_nodash)
