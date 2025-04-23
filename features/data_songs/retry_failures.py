import os
import pandas as pd
import gcsfs
import traceback
import sys
from a import run_with_timeout

INPUT_PATH = "failures.csv"
OUTPUT_PATH = "gs://jacob_weathertunes/data/audio_features/"

# 날짜 기본값 처리
try:
    df_raw = pd.read_csv(INPUT_PATH)
    if "date" not in df_raw.columns:
        df_raw["date"] = pd.Timestamp.today().strftime("%Y%m%d")
    df_fail = df_raw[["track_id", "date"]]
except Exception as e:
    print(f"❌ 실패 목록 로드 실패: {e}")
    sys.exit(1)

track_ids = df_fail["track_id"].dropna().unique().tolist()
target_date = df_fail["date"].iloc[0]

if not track_ids:
    print("⚠️ 재시도할 track_id가 없습니다.")
    sys.exit(0)

print(f"🔁 재시도할 트랙 수: {len(track_ids)}")

fs = gcsfs.GCSFileSystem()
still_failed = []

for i, tid in enumerate(track_ids):
    out_path = f"{OUTPUT_PATH}{tid}.parquet"

    if fs.exists(out_path):
        print(f"⏭️ {tid} 이미 저장됨, 건너뜀")
        continue

    print(f"[{i+1}/{len(track_ids)}] 🔁 재시도 중: {tid}")

    try:
        result = run_with_timeout(tid, timeout=30)
        if result is None:
            still_failed.append((tid, target_date))
            continue

        df_result = pd.DataFrame([result])
        df_result.to_parquet(out_path, index=False)
        print(f"✅ 저장 완료: {out_path}")

    except Exception as e:
        print(f"❌ {tid} 처리 중 예외 발생: {e}")
        traceback.print_exc()
        still_failed.append((tid, target_date))
        continue

# 실패한 항목 다시 저장
if still_failed:
    pd.DataFrame(still_failed, columns=["track_id", "date"]).to_csv(
        INPUT_PATH, index=False
    )
    print("📋 여전히 실패한 항목이 있어 failures.csv 갱신됨")
else:
    if os.path.exists(INPUT_PATH):
        os.remove(INPUT_PATH)
    print("✅ 모든 재시도 성공! failures.csv 삭제됨")
