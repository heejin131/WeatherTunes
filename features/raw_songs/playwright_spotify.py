import sys
import os
import time
import shutil
from pathlib import Path
from datetime import datetime
from playwright.sync_api import sync_playwright
from google.cloud import storage
import json

# 설정
GCS_BUCKET = "jacob_weathertunes"
GCS_PREFIX = "raw/songs_raw"
DOWNLOAD_DIR = Path("/users/jacob/temp/spotify_charts")
COOKIES_PATH = Path("/users/jacob/code/Weathertunes/feature/raw_songs/cookies.json")

def load_cookies(context):
    if COOKIES_PATH.exists():
        cookies = json.loads(COOKIES_PATH.read_text())
        context.add_cookies(cookies)
        print("🍪 쿠키 불러오기 완료")
    else:
        raise FileNotFoundError(f"❌ 쿠키 파일 없음: {COOKIES_PATH}")


def upload_to_gcs(file_path: Path, date_str: str):
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob_path = f"{GCS_PREFIX}/{date_str}.csv"
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(str(file_path))
    print(f"☁️ GCS 업로드 완료: gs://{GCS_BUCKET}/{blob_path}")


def download_chart(date_str: str):
    chart_url = f"https://charts.spotify.com/charts/view/regional-kr-daily/{date_str}"
    print(f"▶️ 접속: {chart_url}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        load_cookies(context)

        page = context.new_page()
        page.goto(chart_url)
        time.sleep(3)

        # 다운로드 버튼 클릭
        try:
            button = page.locator('button[aria-labelledby="csv_download"]')
            button.wait_for(state="visible", timeout=10000)
            print("⬇️ 다운로드 버튼 확인")

            with page.expect_download() as download_info:
                button.click()
            download = download_info.value
            download_path = download.path()

            # 파일명 변경
            downloaded_file = Path(download_path)
            target_path = DOWNLOAD_DIR / f"{date_str}.csv"
            shutil.move(str(downloaded_file), str(target_path))

            print(f"✅ 다운로드 완료: {target_path}")
            # 업로드
            upload_to_gcs(target_path, date_str)

        except Exception as e:
            print("❌ 다운로드 실패:", e)
            screenshot_path = f"screenshot_{date_str}.png"
            page.screenshot(path=screenshot_path, full_page=True)
            print(f"📸 스크린샷 저장됨: {screenshot_path}")

        finally:
            browser.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("⚠️ 날짜를 입력하세요: python script.py YYYY-MM-DD")
        sys.exit(1)

    date_input = sys.argv[1]
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    download_chart(date_input)
