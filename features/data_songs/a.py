from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
import time, random, sys
import os

def scrape_track_data(track_id):
    url = f"https://tunebat.com/Info/track/{track_id}"
    options = uc.ChromeOptions()
    options.add_argument("--headless=new")

    # ✅ 봇 탐지 우회 설정
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    # ✅ 크롬 실행 경로 (인스턴스 VM 기준)
    options.binary_location = "/usr/bin/google-chrome"

    driver = uc.Chrome(options=options, use_subprocess=False)

    try:
        driver.get(url)
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

        # 💡 랜덤 대기 (Cloudflare 챌린지 회피)
        sleep_initial = round(random.uniform(2.5, 5.5), 2)
        print(f"⏳ 페이지 로딩 후 {sleep_initial}초 대기...")
        time.sleep(sleep_initial)

        def get_metric(label):
            try:
                if label == "BPM":
                    el = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, f"//span[text()='{label}']/preceding-sibling::h3"))
                    )
                    return el.text
                else:
                    wrapper = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, f"//span[translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')='{label.lower()}']/ancestor::div[contains(@class, '_1MCwQ')]"))
                    )
                    value_el = wrapper.find_element(By.CLASS_NAME, "ant-progress-text")
                    return value_el.get_attribute("title")
            except Exception as e:
                print(f"❗ {label} 추출 실패: {e}")
                return "N/A"

        print(f"🎧 {track_id} 추출 완료")
        return (track_id, get_metric("BPM"), get_metric("Danceability"), get_metric("Happiness"))

    finally:
        driver.quit()
        sleep_after = round(random.uniform(4, 8), 2)
        print(f"🛌 {sleep_after}초 휴식 중 (봇 방지)")
        time.sleep(sleep_after)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("❌ 날짜 인자 필요: python a.py YYYY-MM-DD")
        sys.exit(1)

    ds_nodash =  sys.argv[1].replace("-", "")
    parquet_path = f"gs://jacob_weathertunes/data/songs_data/dt={ds_nodash}/*.parquet"
    output_path = f"gs://stundrg-bucket/data/audio_features/"

    spark = SparkSession.builder.appName("AudioFeatures").getOrCreate()

    try:
        df_input = spark.read.parquet(parquet_path)
        track_ids = [row.track_id for row in df_input.select("track_id").dropna().distinct().collect()]
    except Exception as e:
        print(f"❌ CSV 로드 실패: {e}")
        sys.exit(1)

    if not track_ids:
        print("⚠️ track_id가 없습니다.")
        sys.exit(0)

    start_time = datetime.now()
    print(f"🚀 총 {len(track_ids)}개 트랙 크롤링 시작")

    results = [
    (tid, scrape_track_data(tid), ds_nodash)
    for tid in track_ids
]

    schema = StructType([
    StructField("track_id", StringType(), True),
    StructField("BPM", IntegerType(), True),
    StructField("Danceability", IntegerType(), True),
    StructField("Happiness", IntegerType(), True),
    StructField("dt", StringType(), True),
])

    df_result = spark.createDataFrame(results, schema)

    try:
        df_result.write.mode("overwrite").partitionBy("dt").save(output_path)
        print(f"✅ 저장 완료: {output_path}")
    except Exception as e:
        print(f"❌ Parquet 저장 실패: {e}")

        print(f"⏱️ 소요 시간: {datetime.now() - start_time}")