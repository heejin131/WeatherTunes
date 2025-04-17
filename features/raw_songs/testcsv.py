import sys
import time
import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

SPOTIFY_EMAIL = "jacob8753@gmail.com"
SPOTIFY_PASSWORD = "!Jacob0503"

def download_spotify_chart(date_str):
    # ✅ 다운로드 경로 설정
    download_dir = "/Users/jacob/temp"
    os.makedirs(download_dir, exist_ok=True)

    options = Options()
    # options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")

    # ✅ 다운로드 경로 설정
    options.add_experimental_option("prefs", {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "directory_upgrade": True
    })

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    wait = WebDriverWait(driver, 20)

    try:
        # 1. 로그인
        login_url = "https://accounts.spotify.com/ko/login?login_hint=jacob8753%40gmail.com&allow_password=1&continue"
        driver.get(login_url)
        print("🔐 로그인 페이지 접속")

        wait.until(EC.presence_of_element_located((By.ID, "login-password"))).send_keys(SPOTIFY_PASSWORD)
        driver.find_element(By.ID, "login-button").click()
        print("✅ 로그인 성공")

        time.sleep(5)

        # 2. 차트 페이지 이동
        chart_url = f"https://charts.spotify.com/charts/view/regional-kr-daily/{date_str}"
        driver.get(chart_url)
        print(f"▶️ 차트 페이지 접속: {chart_url}")

        # 3. 다운로드 버튼 클릭
        download_button = wait.until(EC.element_to_be_clickable((
            By.XPATH, '//button[@aria-labelledby="csv_download"]'
        )))
        download_button.click()
        print(f"📥 다운로드 버튼 클릭됨 → {download_dir} 로 저장 중...")

        # 4. 파일 다운로드 감지 루프
        expected_prefix = f"regional-kr-daily-{date_str}"
        timeout = 20  # 최대 20초 대기
        csv_file_path = None

        for _ in range(timeout):
            for filename in os.listdir(download_dir):
                if filename.startswith(expected_prefix) and filename.endswith(".csv"):
                    csv_file_path = os.path.join(download_dir, filename)
                    break
            if csv_file_path:
                break
            time.sleep(1)

        if csv_file_path:
            print(f"✅ 다운로드 완료: {csv_file_path}")
            return csv_file_path
        else:
            raise FileNotFoundError("❌ 다운로드 파일을 찾지 못했습니다.")

    finally:
        driver.quit()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("사용법: python spotify_csv_clicker.py YYYY-MM-DD")
        sys.exit(1)

    date_input = sys.argv[1]
    try:
        download_spotify_chart(date_input)
    except Exception as e:
        print(f"🚨 에러 발생: {e}")

