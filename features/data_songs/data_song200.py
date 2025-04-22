import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, regexp_replace

if len(sys.argv) < 2:
    raise ValueError("날짜 인자가 필요합니다. 사용 예: spark-submit script.py 2025-04-20")

date = sys.argv[1]
dt = date.replace("-", "")

# Spark 세션 시작
APP_NAME = "songs_200_cleanse"
spark = SparkSession.builder \
    .appName(f"{APP_NAME}_{dt}") \
    .getOrCreate()

print(f"=== Processing input file for date: {date} → saving as dt={dt} ===")

# GCS에서 CSV 읽기
input_path = f"gs://jacob_weathertunes/raw/songs_raw/{date}.csv"
df = spark.read.option("header", True).csv(input_path)

# 전처리
df_cleaned = df.select(
    regexp_replace("uri", "^spotify:track:", "").alias("track_id"),
    col("track_name"),
    col("days_on_chart"),
    col("streams")
).withColumn("dt", lit(dt))

# 저장
output_path = f"gs://jacob_weathertunes/data/songs_top200/"
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
df_cleaned.write.mode("overwrite").partitionBy("dt").parquet(output_path)

print(f"=== ✅ Successfully processed {date} and saved as dt={dt} ===")

spark.stop()
