import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import time, random
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
                        EC.presence_of_element_located((
                            By.XPATH,
                            f"//span[text()='{label}']/preceding-sibling::h3"
                        ))
                    )
                    return el.text
                else:
                    wrapper = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((
                            By.XPATH,
                            f"//span[translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')='{label.lower()}']/ancestor::div[contains(@class, '_1MCwQ')]"
                        ))
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


# ✅ 트랙 ID 리스트
track_ids = [
    "1qCHUbe8BuHymkHuzHEYoi",
    # 추가할 track_id 들...
]

# ✅ 시작 시간 기록
start_time = datetime.now()

# ✅ 크롤링 결과 저장
results = []
for tid in track_ids:
    result = scrape_track_data(tid)
    results.append(result)

# ✅ DataFrame 변환 및 Parquet 저장
df = pd.DataFrame(results)
df.to_parquet("tunebat_features.parquet", index=False)

# ✅ 종료 시간 및 소요 시간 출력
end_time = datetime.now()
elapsed = end_time - start_time
print("✅ parquet 저장 완료: tunebat_features.parquet")
print(f"⏱️ 총 소요 시간: {elapsed}")
