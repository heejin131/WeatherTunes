import sys
import pandas as pd
from pyspark.sql import SparkSession

def convert_to_parquet(raw_data_path: str, save_path: str, dt: str):
    df = pd.read_csv(raw_data_path)
    df["track_id"] = df["uri"].str.extract(r"track:([a-zA-Z0-9]+)")
    df = df[["track_id", "artist_names", "track_name", "days_on_chart", "streams"]]
    df["dt"] = dt

    spark = SparkSession.builder.appName(f"save_song_data_{dt}").getOrCreate()

    spark_df = spark.createDataFrame(df)
    spark_df.write.mode("overwrite").partitionBy("dt").parquet(save_path)

    print(f"✅ 저장 완료: {save_path} (partitioned by dt={dt})")

if __name__ == "__main__":
    ds = sys.argv[1]
    ds_nodash = ds.replace("-", "")
    raw_data_path = f"gs://jacob_weathertunes/raw/songs_raw/{ds}.csv"
    save_path = f"gs://jacob_weathertunes/data/songs_data/"

    convert_to_parquet(raw_data_path, save_path, ds_nodash)
