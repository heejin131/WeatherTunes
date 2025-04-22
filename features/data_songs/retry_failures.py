import pandas as pd
import os
import time
import gcsfs
from a import create_driver, scrape_with_timeout

INPUT_PATH = "failures.csv"
OUTPUT_PATH = "gs://jacob_weathertunes/data/audio_features/"

# 파일 존재 확인
if not os.path.exists(INPUT_PATH):
    print("❌ failures.csv 파일이 없습니다.")
    exit(1)

# 🔧 track_id만 추출
df_fail = pd.read_csv(INPUT_PATH, usecols=["track_id"])
track_ids = df_fail["track_id"].dropna().unique().tolist()

if not track_ids:
    print("⚠️ 처리할 실패 항목이 없습니다.")
    exit(0)

print(f"🔁 재시도할 트랙 수: {len(track_ids)}")

fs = gcsfs.GCSFileSystem()
still_failed = []

driver = create_driver()

for i, tid in enumerate(track_ids):
    out_path = f"{OUTPUT_PATH}{tid}.parquet"

    if fs.exists(out_path):
        print(f"⏭️ {tid} 이미 저장됨, 건너뜀")
        continue

    print(f"[{i+1}/{len(track_ids)}] 🔁 재시도 중: {tid}")

    try:
        result = scrape_with_timeout(driver, tid)
    except RuntimeError:
        print("♻️ 드라이버 커넥션 오류 → 재생성 후 재시도 중...")
        driver.quit()
        time.sleep(3)
        driver = create_driver()
        try:
            result = scrape_with_timeout(driver, tid)
        except Exception as e:
            print(f"❌ {tid} 재시도 실패: {e}")
            result = None

    if result is None:
        still_failed.append(tid)
        continue

    df_result = pd.DataFrame([result])
    df_result.to_parquet(out_path, index=False)
    print(f"✅ 저장 완료: {out_path}")

    if (i + 1) % 20 == 0:
        driver.quit()
        time.sleep(5)
        driver = create_driver()

driver.quit()

# 최종 실패 저장
if still_failed:
    pd.DataFrame(still_failed, columns=["track_id"]).to_csv("failures.csv", index=False)
    print("❗ 여전히 실패한 항목이 존재합니다. failures.csv 갱신됨")
else:
    if os.path.exists("failures.csv"):
        os.remove("failures.csv")
    print("✅ 모든 재시도 성공! failures.csv 삭제됨")
