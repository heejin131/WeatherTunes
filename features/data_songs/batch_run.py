# ✅ 날짜별로 반복 실행하는 루프 예시 (batch_run.py)
from datetime import datetime, timedelta
import subprocess

start_date = datetime.strptime("2024-05-10", "%Y-%m-%d")
end_date = datetime.strptime("2024-05-31", "%Y-%m-%d")

while start_date <= end_date:
    ds = start_date.strftime("%Y%m%d")
    print(f"🚀 {ds} 실행 중...")

    try:
        subprocess.run(["python", "a.py", ds], check=True)
    except subprocess.CalledProcessError:
        print(f"❌ {ds} 작업 실패")

    start_date += timedelta(days=1)
