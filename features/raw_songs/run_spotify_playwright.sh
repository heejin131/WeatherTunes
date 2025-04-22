#!/bin/bash

# 실행할 Python 스크립트 경로
PYTHON_SCRIPT="/Users/jacob/code/WeatherTunes/features/raw_songs/playwright_sportify.py"

# 시작 및 종료 날짜 (인자 2개 필수)
start_date="$1"
end_date="$2"

if [ -z "$start_date" ] || [ -z "$end_date" ]; then
    echo "❗ 날짜 범위가 필요합니다: ./run_spotify_to_gs.sh 2024-03-01 2024-03-31"
    exit 1
fi

# macOS용 date 계산
days_diff=$(( ($(date -jf "%Y-%m-%d" "$end_date" +%s) - $(date -jf "%Y-%m-%d" "$start_date" +%s)) / 86400 ))

echo "📆 실행 범위: $start_date ~ $end_date"
> fail_dates.log  # 실패 로그 초기화

for i in $(seq 0 $days_diff); do
    current_date=$(date -jf "%Y-%m-%d" -v+${i}d "$start_date" +%Y-%m-%d)
    echo "▶️ 실행: $current_date"

    python "$PYTHON_SCRIPT" "$current_date"
    if [ $? -ne 0 ]; then
        echo "❌ 실패: $current_date"
        echo "$current_date" >> fail_dates.log
    fi

    # 🎯 랜덤 딜레이: 5~10초
    delay=$((RANDOM % 6 + 5))
    echo "⏳ 대기 ${delay}초..."
    sleep $delay
done

echo "🏁 전체 작업 완료"
echo "📄 실패 날짜 로그: fail_dates.log"
