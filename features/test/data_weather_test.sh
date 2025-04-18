#!/usr/bin/env bash
set -euo pipefail

AUTH="aF29N6-AQoKdvTevgNKChw"
BASE="https://apihub.kma.go.kr/api/typ01/url/kma_sfctm2.php"

STATIONS=108

# 테스트할 시각 (YYYYMMDDHHMM)
TIMES=(202504010000 202504011200 202504020600 202504021800)

# 저장 경로
OUTDIR="./test_weather_txt"
mkdir -p "$OUTDIR"

for stn in "${STATIONS[@]}"; do
  for tm in "${TIMES[@]}"; do
    filename="${OUTDIR}/weather_${tm}_stn${stn}.txt"
    curl -s "${BASE}?authKey=${AUTH}&tm=${tm}&stn=${stn}" \
      -o "$filename"
    echo "Saved $filename"
  done
done
EOF
