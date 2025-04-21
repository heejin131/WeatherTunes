from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.utils import AnalysisException
from datetime import datetime, timedelta
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium_stealth import stealth
import time, random, sys, os


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
    options.add_argument("user-agent=Mozilla/5.0 ...")
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
        print(f"\n⏳ [{track_id}] 페이지 로딩 후 {sleep_initial}초 대기...", flush=True)
        time.sleep(sleep_initial)

        def get_metric(label):
            try:
                print(f"🔍 [{track_id}] {label} 추출 시도 중...", flush=True)

                if label == "BPM":
                    el = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, f"//span[text()='{label}']/preceding-sibling::h3"))
                    )
                    value = el.text
                else:
                    wrapper = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.XPATH,
                            f"//span[translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')='{label.lower()}']/ancestor::div[contains(@class, '_1MCwQ')]"))
                    )
                    value_el = wrapper.find_element(By.CLASS_NAME, "ant-progress-text")
                    value = value_el.get_attribute("title")

                print(f"✅ [{track_id}] {label} 원본값: {value} → 정수 변환: {to_int_safe(value)}", flush=True)
                return value
            except Exception as e:
                print(f"❌ [{track_id}] {label} 추출 실패: {e}", flush=True)
                return "N/A"

        result = (
            track_id,
            to_int_safe(get_metric("BPM")),
            to_int_safe(get_metric("Danceability")),
            to_int_safe(get_metric("Happiness"))
        )

        print(f"📦 최종 결과: {result}", flush=True)
        return result

    finally:
        driver.quit()
        sleep_after = round(random.uniform(4, 7), 2)
        print(f"🛌 {sleep_after}초 휴식 중 (봇 방지)", flush=True)
        time.sleep(sleep_after)


def scrape_track_data_with_retry(track_id, retries=2):
    for attempt in range(retries + 1):
        try:
            return scrape_track_data(track_id)
        except Exception as e:
            print(f"⚠️ {track_id} 재시도 {attempt+1}/{retries + 1}: {e}", flush=True)
            time.sleep(random.uniform(1, 3))
    return (track_id, None, None, None)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("❌ 날짜 인자 필요: python a.py YYYYMMDD", flush=True)
        sys.exit(1)

    ds = sys.argv[1]
    parquet_path = f"gs://jacob_weathertunes/data/songs_top200/dt={ds}/*.parquet"
    prev_path = f"gs://stundrg-bucket/data/audio_features/dt={ds}/*.parquet"

    spark = SparkSession.builder \
        .appName("AudioFeatures") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()

    try:
        # 🔹 현재 트랙 목록
        df_today = spark.read.parquet(parquet_path)
        today_ids_df = df_today.select("track_id").dropna().distinct()

        # 🔹 이미 수집된 트랙 필터링
        try:
            df_existing = spark.read.parquet(prev_path)
            prev_ids_df = df_existing.select("track_id").dropna().distinct()
            today_ids_df = today_ids_df.join(prev_ids_df, on="track_id", how="left_anti")
            print(f"✅ 중복 제거 후 track_id 개수: {today_ids_df.count()}", flush=True)
        except AnalysisException:
            print("ℹ️ 이전 저장 데이터 없음. 전체 수집 진행", flush=True)

        # track_ids = [row.track_id for row in today_ids_df.collect()]
        track_ids = [row.track_id for row in today_ids_df.collect()[:5]]  # 🔹 테스트용 5개만

    except Exception as e:
        print(f"❌ Parquet 로드 실패: {e}", flush=True)
        sys.exit(1)

    if not track_ids:
        print("⚠️ 크롤링할 track_id가 없습니다.", flush=True)
        sys.exit(0)

    print(f"\n🚀 총 {len(track_ids)}개 track_id 추출 시작", flush=True)

    results = [
        scrape_track_data_with_retry(tid)
        for tid in track_ids
    ]

    schema = StructType([
        StructField("track_id", StringType(), True),
        StructField("BPM", IntegerType(), True),
        StructField("Danceability", IntegerType(), True),
        StructField("Happiness", IntegerType(), True),
    ])

    df_result = spark.createDataFrame(results, schema)
    df_result.show(truncate=False)

    try:
        df_result.write.mode("overwrite").save("gs://stundrg-bucket/data/audio_features/")
        print(f"✅ 저장 완료!", flush=True)
    except Exception as e:
        print(f"❌ 저장 실패: {e}", flush=True)

    print(f"⏱️ 전체 소요 시간: {datetime.now()}")
