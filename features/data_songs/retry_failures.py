import os
import pandas as pd
import multiprocessing as mp
import traceback
from a import run_with_timeout
import undetected_chromedriver as uc
import gcsfs

output_path = "gs://jacob_weathertunes/data/audio_features/"
failures_file = "failures.csv"
fs = gcsfs.GCSFileSystem()

def retry_failed_tracks():
    if not os.path.exists(failures_file):
        print("❌ 실패 목록 파일이 존재하지 않습니다.")
        return

    df = pd.read_csv(failures_file)
    if "track_id" not in df or "date" not in df:
        print("❌ 실패 목록 파일 포맷이 올바르지 않습니다.")
        return

    new_failures = []

    for i, row in df.iterrows():
        tid = row["track_id"]
        date = row["date"]
        out_path = f"{output_path}{tid}.parquet"

        try:
            if fs.exists(out_path):
                print(f"⏭️ {tid} 이미 저장됨, 건너뜀")
                continue

            print(f"[{i+1}/{len(df)}] 🔁 {tid} ({date}) 재시도 중...")

            result = run_with_timeout(tid, timeout=30)
            if result is None:
                new_failures.append({"track_id": tid, "date": date})
                continue

            pd.DataFrame([result]).to_parquet(out_path, index=False)
            print(f"✅ 저장 완료: {out_path}")

        except Exception as e:
            print(f"❌ {tid} 재시도 실패: {e}")
            traceback.print_exc()
            new_failures.append({"track_id": tid, "date": date})

    if new_failures:
        pd.DataFrame(new_failures).to_csv(failures_file, index=False)
        print("📝 실패 항목 다시 저장 완료.")
    else:
        os.remove(failures_file)
        print("🎉 모든 실패 항목 재처리 완료. 파일 삭제됨.")

if __name__ == "__main__":
    retry_failed_tracks()
