#!/bin/bash

PYTHON_SCRIPT="/Users/jacob/code/WeatherTunes/features/raw_songs/playwright_spotify.py"
FAIL_LOG="fail_dates.log"
RETRY_LOG="retry_fail_dates.log"

if [ ! -f "$FAIL_LOG" ]; then
    echo "❗ $FAIL_LOG 파일이 존재하지 않습니다."
    exit 1
fi

echo "🔁 실패 날짜 재시도 시작..."
> "$RETRY_LOG"  # 재실패 로그 초기화

while read -r date; do
    echo "▶️ 재시도: $date"
    python "$PYTHON_SCRIPT" "$date"
    if [ $? -ne 0 ]; then
        echo "❌ 재시도 실패: $date"
        echo "$date" >> "$RETRY_LOG"
    fi

    # 🔁 재시도도 랜덤 딜레이
    delay=$((RANDOM % 6 + 5))
    echo "⏳ 대기 ${delay}초..."
    sleep $delay
done < "$FAIL_LOG"

echo "✅ 재시도 완료"
echo "📄 재실패 로그: $RETRY_LOG"
