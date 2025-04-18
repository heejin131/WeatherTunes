import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import time, random
import sys, os
from datetime import datetime

def scrape_track_data(track_id):
    url = f"https://tunebat.com/Info/track/{track_id}"
    options = uc.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    driver = uc.Chrome(options=options, use_subprocess=True)

    try:
        driver.get(url)
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(3)

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

        bpm = get_metric("BPM")
        dance = get_metric("Danceability")
        happy = get_metric("Happiness")

        print(f"🎧 {track_id} 추출 완료")
        return {
            "track_id": track_id,
            "BPM": bpm,
            "Danceability": dance,
            "Happiness": happy
        }

    finally:
        driver.quit()
        time.sleep(random.uniform(3, 6))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("❌ 날짜 인자가 필요합니다. 예: python a.py 2025-04-18")
        sys.exit(1)

    date_str = sys.argv[1]
    songs_path = f"/home/airflow/gcs/data/songs_top200/dt={date_str}/songs_top200.parquet"
    
    if not os.path.exists(songs_path):
        print(f"❌ songs_top200 파일을 찾을 수 없습니다: {songs_path}")
        sys.exit(1)

    df_input = pd.read_parquet(songs_path)
    track_ids = df_input["track_id"].dropna().unique().tolist()

    start_time = datetime.now()
    results = []

    for tid in track_ids:
        result = scrape_track_data(tid)
        results.append(result)

    df_result = pd.DataFrame(results)
    output_path = f"/home/airflow/gcs/data/audio_features/dt={date_str}/audio_features.parquet"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_result.to_parquet(output_path, index=False)

    end_time = datetime.now()
    print(f"✅ 저장 완료: {output_path}")
    print(f"⏱️ 총 소요 시간: {end_time - start_time}")
