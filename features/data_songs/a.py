from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium_stealth import stealth
from datetime import datetime
import time, random, sys
import os

def to_int_safe(value):
    try:
        return int(value)
    except:
        return None

def scrape_track_data(track_id):
    url = f"https://tunebat.com/Info/track/{track_id}"
    options = uc.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--no-first-run")
    options.add_argument("--no-default-browser-check")
    options.add_argument("--disable-background-networking")
    options.add_argument("--disable-sync")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    options.binary_location = "/usr/bin/google-chrome"

    driver = uc.Chrome(options=options, use_subprocess=False)

    stealth(driver,
        languages=["en-US", "en"],
        vendor="Google Inc.",
        platform="Win32",
        webgl_vendor="Intel Inc.",
        renderer="Intel Iris OpenGL Engine",
        fix_hairline=True,
    )

    try:
        driver.get(url)
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        sleep_initial = round(random.uniform(2.5, 5.5), 2)
        print(f"\n⏳ 페이지 로딩 후 {sleep_initial}초 대기...")
        time.sleep(sleep_initial)

        def get_metric(label):
            try:
                print(f"🔍 [{track_id}] {label} 추출 시도 중...")

                if label == "BPM":
                    el = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, f"//span[text()='{label}']/preceding-sibling::h3"))
                    )
                    value = el.text
                else:
                    wrapper = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, f"//span[translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')='{label.lower()}']/ancestor::div[contains(@class, '_1MCwQ')]"))
                    )
                    value_el = wrapper.find_element(By.CLASS_NAME, "ant-progress-text")
                    value = value_el.get_attribute("title")

                print(f"✅ [{track_id}] {label} 원본값: {value} → 정수 변환: {to_int_safe(value)}")
                return value
            except Exception as e:
                print(f"❌ [{track_id}] {label} 추출 실패: {e}")
                return "N/A"

        result = (
            track_id,
            to_int_safe(get_metric("BPM")),
            to_int_safe(get_metric("Danceability")),
            to_int_safe(get_metric("Happiness"))
        )

        print(f"📦 최종 결과: {result}")
        return result

    finally:
        driver.quit()
        sleep_after = round(random.uniform(4, 8), 2)
        print(f"🛌 {sleep_after}초 휴식 중 (봇 방지)")
        time.sleep(sleep_after)

def scrape_track_data_with_retry(track_id, retries=2):
    for attempt in range(1, retries + 2):
        try:
            return scrape_track_data(track_id)
        except Exception as e:
            print(f"⚠️ {track_id} 재시도 {attempt}/{retries + 1}: {e}")
            time.sleep(random.uniform(2, 4))
    return (track_id, None, None, None)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("❌ 날짜 인자 필요: python a.py YYYY-MM-DD")
        sys.exit(1)

    ds = sys.argv[1]
    ds_nodash = ds.replace("-", "")
    parquet_path = f"gs://jacob_weathertunes/data/songs_top200/dt={ds_nodash}/*.parquet"
    output_path = f"gs://stundrg-bucket/data/audio_features/"

    spark = SparkSession.builder \
        .appName("AudioFeatures") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()

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
        (*scrape_track_data_with_retry(tid), ds_nodash)
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
