import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, broadcast

def merge_data(weather_path: str, songs_path: str, audio_features_path: str, save_path: str, dt: str):
    spark = SparkSession.builder.appName(f"merge_meta_{dt}") \
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
            .getOrCreate()
    
    weather_df = spark.read.parquet(weather_path)
    songs_df = spark.read.parquet(songs_path) \
        .filter("days_on_chart < 30") \
        .select("track_id", "artist_names", "track_name", "streams")
    audio_features_df = spark.read.parquet(audio_features_path)
    
    merged_song_df = songs_df.join(broadcast(audio_features_df), on="track_id", how="left")
    result_df = merged_song_df.join(weather_df, on="dt", how="left")
    result_df = result_df.withColumn("dt", lit(dt))
    
    result_df.write.mode("overwrite") \
        .partitionBy("dt", "weather_code", "temp_code") \
        .parquet(save_path)
        
    print(f"✅ 저장 완료: {save_path} (dt={dt})")

if __name__ == "__main__":
    ds_nodash = sys.argv[1]
    weather_path = f"gs://jacob_weathertunes/data/weather_data/dt={ds_nodash}/"
    songs_path = f"gs://jacob_weathertunes/data/songs_top200/dt={ds_nodash}/"
    audio_features_path = f"gs://jacob_weathertunes/data/audio_features/"
    save_path = f"gs://jacob_weathertunes/meta/"
    
    merge_data(weather_path, songs_path, audio_features_path, save_path, ds_nodash)