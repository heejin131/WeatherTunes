import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

def generate_meta_profile(date: str):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/gmlwls5168/keys/praxis-zoo-455400-d2-e1891bf1943d.json"

    spark = SparkSession.builder \
        .appName("MetaProfileGenerator") \
        .getOrCreate()

    # GCS 경로 설정
    base = "gs://nijin-bucket/dummy"
    weather_path = f"{base}/weather_daily/partition={date}/"
    songs_path = f"{base}/songs_top200/partition={date}/"
    audio_path = f"{base}/audio_features/partition={date}/"

    # 데이터 로드 (parquet 파일이 한 개만 존재한다고 가정)
    weather_df = spark.read.parquet(weather_path)
    songs_df = spark.read.parquet(songs_path)
    audio_df = spark.read.parquet(audio_path)

    # 병합
    merged = songs_df.join(audio_df, on="track_id", how="inner")

    # 날씨 값 추출 및 컬럼 추가
    weather_row = weather_df.limit(1).collect()[0]

    merged = merged.withColumn("weather_main", lit(weather_main))
    merged = merged.withColumn("temp_band", lit(temp_band))
    merged = merged.withColumn("date", lit(date))
    # ✅ GCS에 저장할 경로
    output_path = f"gs://nijin-bucket/meta/meta_profile/{date}/weather_main={weather_main}/temp_band={temp_band}/"

    # 저장
    merged.write.mode("overwrite").partitionBy("date").parquet(output_path)

    print(f"✅ 저장 완료: {output_path}")

if __name__ == "__main__":
    import sys
    main(sys.argv[1])

    # BigQuery 업로드
    # client = bigquery.Client()
    # table_id = "praxis-zoo-455400-d2.meta_dataset.meta_profile"
    # job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    # job = client.load_table_from_dataframe(merged, table_id, job_config=job_config)
    # job.result()

    # print(f"✅ 업로드 완료: {date}")
