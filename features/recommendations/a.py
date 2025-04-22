import sys
import os
import requests
import pandas as pd
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, pow, abs, avg, rand

load_dotenv()
BASE_PATH = "gs://jacob_weathertunes"

def create_weather_code(sd_day, sd_hr3, rn, rn_day, ca_tot) -> int:
    if (pd.notna(sd_day) and sd_day > 0) or (pd.notna(sd_hr3) and sd_hr3 > 0):
        return 3  # 눈
    elif (pd.notna(rn_day) and rn > 0) or (pd.notna(rn_day) and rn_day > 0):
        return 2  # 비
    elif pd.notna(ca_tot) and ca_tot >= 5:
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

def get_codes(text_data: str):
    from io import StringIO
    
    lines = text_data.splitlines()
    data_lines = [line for line in lines if line.strip() and not line.startswith("#")]
    cleaned = "\n".join(data_lines)
    
    names = [
    'TM', 'STN', 'WD', 'WS', 'GST_WD', 'GST_WS', 'GST_TM',
    'PA', 'PS', 'PT', 'PR', 'TA', 'TD', 'HM', 'PV', 'RN',
    'RN_DAY', 'RN_JUN', 'RN_INT', 'SD_HR3', 'SD_DAY', 'SD_TOT',
    'WC', 'WP', 'WW', 'CA_TOT', 'CA_MID', 'CH_MIN', 'CT',
    'CT_TOP', 'CT_MID', 'CT_LOW', 'VS', 'SS', 'SI', 'ST_GD',
    'TS', 'TE_005', 'TE_01', 'TE_02', 'TE_03', 'ST_SEA', 'WH',
    'BF', 'IR', 'IX'
    ]
    
    df = pd.read_csv(StringIO(cleaned), sep=r"\s+", header=None)
    df.columns = names
    df.replace(["-9", -9, "-9.0", -9.0, "-"], value=pd.NA, inplace=True)
    
    weather_code = create_weather_code(
            df["SD_DAY"].item(),
            df["SD_HR3"].item(),
            df["RN"].item(),
            df["RN_DAY"].item(),
            df["CA_TOT"].item()
    )
    temp_code = create_temp_code(df["TA"].item())
    
    return weather_code, temp_code

def load_weather_now(ds: str):
    ds_nodash = ds.replace("-", "")
    auth_key = os.getenv("WEATHER_API_KEY")
    if not auth_key:
        raise ValueError("❌ WEATHER_API_KEY가 없습니다.")
    
    url = (
        f"https://apihub.kma.go.kr/api/typ01/url/kma_sfctm2.php?"
        f"tm={ds_nodash}0700&stn=108&authKey={auth_key}"
    )
    response = requests.get(url)
    response.encoding = "utf-8"

    if response.status_code == 200:
        return get_codes(response.text)
    else:
        print(f"❌ 요청 실패! 상태코드: {response.status_code}")
        print(response.text)

def recommend_random_tracks(track_info_path: str, save_path: str):
    print("⚠️ 해당 조건의 메타데이터 없음. 랜덤 추천으로 대체합니다.")

    # songs_df = spark.read.parquet(track_info_path) \
    #     .select("track_id", "artist_names", "track_name") \
    #     .dropDuplicates(["track_id"])
    songs_df = spark.read.option("header", True).csv(track_info_path) \
            .select("track_id", "artist_names", "track_name") \
            .dropDuplicates(["track_id"])

    df = songs_df.orderBy(rand()).limit(3)

    df.select("track_id", "artist_names", "track_name") \
        .toPandas().to_json(save_path, orient="records", force_ascii=False)

    print(f"✅ 랜덤 추천 결과를 {save_path}에 저장 완료")

def recommend_tracks(meta_path: str, track_info_path: str, audio_features_path: str, save_path: str, weather_code: str, temp_code: str, ds: str):
    spark = SparkSession.builder.appName(f"recommend_tracks_{ds}") \
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
            .getOrCreate()

    meta_df = spark.read.parquet(meta_path) \
        .select("BPM", "danceability", "happiness")

    if meta_df.rdd.isEmpty():
        recommend_random_tracks(track_info_path, save_path)
        return

    avg_df = meta_df.agg(
        avg("BPM").alias("BPM"),
        avg("danceability").alias("danceability"),
        avg("happiness").alias("happiness")
    )
    avg_vals = avg_df.first()
    
    audio_features_df = spark.read.parquet(audio_features_path) \
            .select("track_id", "BPM", "danceability", "happiness")
    
    df_with_distance = audio_features_df.withColumn(
        "dinstance",
        pow((col("BPM") - lit(float(avg_vals["BPM"]))), 2) +
        pow((col("danceability") - lit(float(avg_vals["danceability"]))), 2) +
        pow((col("happiness") - lit(float(avg_vals["happiness"]))), 2)
    )
    
    top3_df = df_with_distance.orderBy(col("dinstance")).limit(3)
    # songs_df = spark.read.parquet(track_info_path) \
    #     .select("track_id", "artist_names", "track_name") \
    #     .dropDuplicates(["track_id"])
    songs_df = spark.read.option("header", True).csv(track_info_path) \
            .select("track_id", "artist_names", "track_name") \
            .dropDuplicates(["track_id"])
        
    result_df = top3_df.join(songs_df, on="track_id", how="left") \
        .select("track_id", "artist_names", "track_name")

    result_df.toPandas().to_json(save_path, orient="records", force_ascii=False)
    
    print(f"✅ top 3 데이터를 {save_path}로 저장 완료")

if __name__ == "__main__":
    ds = sys.argv[1]
    weather_code, temp_code = load_weather_now(ds)

    meta_path = f"{BASE_PATH}/meta/dt=*/weather_code={weather_code}/temp_code={temp_code}/"
    # track_info_path = f"{BASE_PATH}/meta/"
    track_info_path = f"{BASE_PATH}/data/songs_top200/"
    audio_features_path = f"{BASE_PATH}/data/audio_features"
    save_path = f"{BASE_PATH}/tmp/recommend_{ds}.json"
    
    recommend_tracks(meta_path, track_info_path, audio_features_path, save_path, weather_code, temp_code, ds)
