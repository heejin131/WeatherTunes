import os
import pandas as pd
import gcsfs
import re
import sys, traceback
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium_stealth import stealth
import undetected_chromedriver as uc
import time, random
import multiprocessing as mp
from datetime import datetime

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

        return {
            "track_id": track_id,
            "BPM": to_int_safe(get_metric("BPM")),
            "Danceability": to_int_safe(get_metric("Danceability")),
            "Happiness": to_int_safe(get_metric("Happiness"))
        }

    finally:
        driver.quit()
        time.sleep(random.uniform(4, 7))

def worker(track_id, return_dict):
    try:
        result = scrape_track_data(track_id)
        return_dict["result"] = result
    except Exception as e:
        return_dict["error"] = str(e)

def run_with_timeout(track_id, timeout=30):
    manager = mp.Manager()
    return_dict = manager.dict()
    p = mp.Process(target=worker, args=(track_id, return_dict))
    p.start()
    p.join(timeout)

    if p.is_alive():
        p.terminate()
        p.join()
        print(f"❌ {track_id} 타임아웃으로 중단됨")
        return None
    if "result" in return_dict:
        return return_dict["result"]
    else:
        print(f"❌ {track_id} 오류 발생: {return_dict.get('error')}")
        return None

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
        print(f"📦 입력 날짜 인자: {target_date}")
    else:
        target_date = get_latest_partition(input_base)
        print(f"📅 최신 날짜 자동 선택: {target_date}")

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

    print(f"🚀 총 {len(track_ids)}개 트랙 수집 시작")

    fs = gcsfs.GCSFileSystem()
    failures = []

    for i, tid in enumerate(track_ids):
        try:
            out_path = f"{output_path}{tid}.parquet"

            if fs.exists(out_path):
                print(f"⏭️ {tid} 이미 저장됨, 건너뜀")
                continue

            print(f"[{i+1}/{len(track_ids)}] 🎵 {tid} 크롤링 중...")

            result = run_with_timeout(tid, timeout=30)
            if result is None:
                failures.append((tid, target_date))
                continue

            df_result = pd.DataFrame([result])
            df_result.to_parquet(out_path, index=False)

            print(f"✅ 저장 완료: {out_path}")

        except Exception as e:
            print(f"❌ {tid} 처리 중 예외 발생: {e}")
            traceback.print_exc()
            failures.append((tid, target_date))
            continue

    print(f"\n🎉 모든 작업 완료! 저장 경로: {output_path}")

    if failures:
        print("\n📋 실패한 track_id 목록:")
        for tid, _ in failures:
            print("-", tid)

        mode = "a" if os.path.exists("failures.csv") else "w"
        header = not os.path.exists("failures.csv")
        pd.DataFrame(failures, columns=["track_id", "date"]).to_csv("failures.csv", index=False, mode=mode, header=header)
        print("📝 실패 목록을 failures.csv 로 저장했습니다.")
    else:
        print("✅ 모든 track_id 성공적으로 처리되었습니다.")
