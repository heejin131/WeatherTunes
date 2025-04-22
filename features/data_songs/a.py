import os, sys, time, random, re, traceback
import pandas as pd
import gcsfs
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium_stealth import stealth
import undetected_chromedriver as uc
import signal

def to_int_safe(value):
    try:
        return int(value)
    except:
        return None

class TimeoutException(Exception): pass

def timeout_handler(signum, frame):
    raise TimeoutException()

signal.signal(signal.SIGALRM, timeout_handler)

def create_driver():
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
    stealth(driver, languages=["en-US", "en"], vendor="Google Inc.",
            platform="Win32", webgl_vendor="Intel Inc.",
            renderer="Intel Iris OpenGL Engine", fix_hairline=True)
    return driver

def scrape_track_data(driver, track_id):
    url = f"https://tunebat.com/Info/track/{track_id}"
    try:
        driver.get(url)
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(random.uniform(2.5, 5.5))

        def get_metric(label):
            try:
                if label == "BPM":
                    el = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, f"//span[text()='{label}']/preceding-sibling::h3"))
                    )
                    return el.text
                else:
                    wrapper = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.XPATH,
                            f"//span[translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')='{label.lower()}']/ancestor::div[contains(@class, '_1MCwQ')]"))
                    )
                    value_el = wrapper.find_element(By.CLASS_NAME, "ant-progress-text")
                    return value_el.get_attribute("title")
            except:
                return "N/A"
        
        bpm = to_int_safe(get_metric("BPM"))
        danceability = to_int_safe(get_metric("Danceability"))
        happiness = to_int_safe(get_metric("Happiness"))
        
        if bpm is not None and danceability is not None and happiness is not None:
            return {
                "track_id": track_id,
                "BPM": bpm,
                "Danceability": danceability,
                "Happiness": happiness
            }
    except Exception as e:
        print(f"❌ {track_id} 크롤링 중 오류 발생: {e}")
    return None

def scrape_with_timeout(driver, track_id, timeout=30):
    import signal
    class TimeoutException(Exception): pass
    def timeout_handler(signum, frame):
        raise TimeoutException()

    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(timeout)
    try:
        return scrape_track_data(driver, track_id)
    except TimeoutException:
        print(f"⏰ {track_id} 타임아웃 발생")
        return None
    except Exception as e:
        print(f"⚠️ {track_id} 일반 오류: {e}")
        if "Connection refused" in str(e) or "Max retries exceeded" in str(e):
            raise RuntimeError("Driver connection broken")
        return None
    finally:
        signal.alarm(0)


def get_latest_partition(bucket_path):
    fs = gcsfs.GCSFileSystem()
    try:
        dirs = fs.ls(bucket_path)
        partitions = [re.search(r'dt=(\d{8})', d) for d in dirs]
        dates = [match.group(1) for match in partitions if match]
        return max(dates) if dates else None
    except Exception as e:
        print(f"❌ GCS 파티션 목록 조회 실패: {e}")
        return None

if __name__ == "__main__":
    input_base = "gs://jacob_weathertunes/data/songs_top200/"
    output_path = "gs://jacob_weathertunes/data/audio_features/"

    if len(sys.argv) == 2:
        target_date = sys.argv[1]
    else:
        target_date = get_latest_partition(input_base)

    if not target_date:
        print("❌ 유효한 날짜를 찾을 수 없습니다.")
        sys.exit(1)

    input_path = f"{input_base}dt={target_date}/"
    print(f"📥 입력 경로: {input_path}")

    try:
        df_input = pd.read_parquet(input_path)
        track_ids = df_input["track_id"].dropna().unique().tolist()
    except Exception as e:
        print(f"❌ 입력 파일 로드 실패: {e}")
        traceback.print_exc()
        sys.exit(1)

    if not track_ids:
        print("⚠️ track_id가 없습니다.")
        sys.exit(0)

    fs = gcsfs.GCSFileSystem()
    failures = []
    driver = create_driver()

    for i, tid in enumerate(track_ids):
        out_path = f"{output_path}{tid}.parquet"

        if fs.exists(out_path):
            print(f"⏭️ {tid} 이미 저장됨, 건너뜀")
            continue

        print(f"[{i+1}/{len(track_ids)}] 🎵 {tid} 크롤링 중...")

        result = scrape_with_timeout(driver, tid)

        if result is None:
            failures.append((tid, target_date))
            continue

        pd.DataFrame([result]).to_parquet(out_path, index=False)
        print(f"✅ 저장 완료: {out_path}")

        # 💡 일정 개수마다 driver 재시작
        if (i + 1) % 20 == 0:
            driver.quit()
            time.sleep(5)
            driver = create_driver()

    driver.quit()

    print(f"\n🎉 모든 작업 완료! 저장 경로: {output_path}")

    if failures:
        pd.DataFrame(failures, columns=["track_id", "date"]).to_csv("failures.csv", index=False, mode="a", header=not os.path.exists("failures.csv"))
        print("📝 실패 목록을 failures.csv 로 저장했습니다.")
    else:
        print("✅ 모든 track_id 성공적으로 처리되었습니다.")
