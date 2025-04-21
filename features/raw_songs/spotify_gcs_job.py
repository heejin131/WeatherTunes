import os
import time
import random
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from google.cloud import storage

# 설정
SPOTIFY_EMAIL = "jacob8753@gmail.com"
SPOTIFY_PASSWORD = "Password"
DOWNLOAD_DIR = "/home/jacob8753/temp/spotify_charts/"
GCS_BUCKET = "jacob_weathertunes"
GCS_PREFIX = "raw/songs_raw"

def setup_driver(download_dir):
    os.makedirs(download_dir, exist_ok=True)
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("window-size=1920x1080")
    options.add_argument("user-agent=Mozilla/5.0")
    options.add_experimental_option("prefs", {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "directory_upgrade": True
    })
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    return driver

def download_spotify_chart(date_str):
    driver = setup_driver(DOWNLOAD_DIR)
    wait = WebDriverWait(driver, 30)
    sleep_duration = random.uniform(3, 7)
    try:
        # 1. 로그인
        login_url = "https://accounts.spotify.com/ko/login?login_hint=jacob8753%40gmail.com&allow_password=1&continue"
        driver.get(login_url)
        wait.until(EC.presence_of_element_located((By.ID, "login-password"))).send_keys(SPOTIFY_PASSWORD)
        driver.find_element(By.ID, "login-button").click()
        print("✅ 로그인 완료")
        time.sleep(5)

        # 2. 차트 페이지 접속
        chart_url = f"https://charts.spotify.com/charts/view/regional-kr-daily/{date_str}"
        driver.get(chart_url)
        print(f"▶️ 차트 접속: {chart_url}")
        time.sleep(5)

        # 3. 강제 스크롤 & UI 요소 제거
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)

        driver.execute_script("""
            let el1 = document.getElementById("onetrust-policy-text");
            if (el1) el1.style.display = "none";
            let el2 = document.querySelector('.ChartsHeader__Grid-sc-1rla63q-1');
            if (el2) el2.style.display = "none";
            let el3 = document.querySelector('header[data-testid="charts-header"]');
            if (el3) el3.style.display = "none";
        """)
        print("🧹 UI 요소 숨김 처리 완료")

        # 4. 다운로드 버튼 클릭
        try:
            download_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[@aria-labelledby='csv_download']")))
            download_button.click()
            print("📥 기본 클릭 성공")
        except Exception as e:
            print(f"⚠️ 일반 클릭 실패, JS fallback 시도 중... ({e})")
            try:
                driver.execute_script("document.querySelector('[aria-labelledby=\"csv_download\"]').click();")
                print("✅ JS fallback 클릭 시도됨")
            except Exception as js_e:
                raise Exception("❌ JS 클릭도 실패함") from js_e

        # 5. 다운로드 감지
        expected_prefix = f"regional-kr-daily-{date_str}"
        timeout = 30
        downloaded_file = None

        for _ in range(timeout):
            files = os.listdir(DOWNLOAD_DIR)
            print(f"📁 현재 파일 목록: {files}")
            for filename in files:
                if filename.startswith(expected_prefix) and filename.endswith(".csv"):
                    downloaded_file = os.path.join(DOWNLOAD_DIR, filename)
                    break
            if downloaded_file:
                break
            time.sleep(1)

        if not downloaded_file:
            raise FileNotFoundError(f"❌ [{date_str}] 다운로드 실패")

        print(f"✅ 다운로드 완료: {downloaded_file}")
        return downloaded_file

    finally:
        driver.quit()

def upload_to_gcs(local_file, bucket_name, blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(local_file)
    print(f"✅ GCS 업로드 완료: gs://{bucket_name}/{blob_name}")

# DAG에서 호출
def run_spotify_job(ds: str, **kwargs):
    print(f"📆 실행 날짜: {ds}")
    local_file = download_spotify_chart(ds)
    blob_path = f"{GCS_PREFIX}/{ds}.csv"
    upload_to_gcs(local_file, GCS_BUCKET, blob_path)

