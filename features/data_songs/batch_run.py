from datetime import datetime, timedelta
import subprocess

start_date = datetime.strptime("2024-08-24", "%Y-%m-%d")
end_date = datetime.strptime("2025-04-01", "%Y-%m-%d")

while start_date <= end_date:
    ds = start_date.strftime("%Y%m%d")
    print(f"🚀 {ds} 실행 중...")

    try:
        subprocess.run(["python", "a.py", ds], check=True)
    except subprocess.CalledProcessError:
        print(f"❌ {ds} 작업 실패")

    # ✅ 수집 후 실패 항목 재시도
    try:
        subprocess.run(["python", "retry_failures.py"], check=True)
    except subprocess.CalledProcessError:
        print("❌ retry_failures.py 실행 실패")

    start_date += timedelta(days=1)
