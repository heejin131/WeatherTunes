import time
import json
from pathlib import Path
from playwright.sync_api import sync_playwright

# 설정
SPOTIFY_EMAIL = "jacob8753@gmail.com"
SPOTIFY_PASSWORD = "password"
COOKIES_PATH = Path("/users/jacob/code/Weathertunes/feature/raw_songs/cookies.json")

def save_cookies():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()

        print("🌐 로그인 페이지 접속 중...")
        login_url = "https://accounts.spotify.com/ko/login?login_hint=jacob8753%40gmail.com&allow_password=1&continue"
        page.goto(login_url)

        print("⌨️ 로그인 정보 입력 중...")
        page.fill("#login-username", SPOTIFY_EMAIL)
        page.fill("#login-password", SPOTIFY_PASSWORD)
        page.click("#login-button")

        print("⏳ 로그인 후 리디렉션 대기 중...")
        page.wait_for_load_state("networkidle")
        time.sleep(5)  # 추가 대기 (2FA나 리디렉션 등 대응)

        cookies = context.cookies()
        with open(COOKIES_PATH, "w") as f:
            json.dump(cookies, f)

        print(f"✅ 쿠키 저장 완료: {COOKIES_PATH}")

        context.close()
        browser.close()


if __name__ == "__main__":
    save_cookies()
