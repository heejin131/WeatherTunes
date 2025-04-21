#!/bin/bash

DATE=$1

if [ -z "$DATE" ]; then
  echo "❌ 날짜 인자가 필요합니다. 예: 2025-01-22"
  exit 1
fi

echo "🚀 [INFO] Spark job 시작 for $DATE"

/home/jacob8753/app/spark-3.5.1-bin-hadoop3/bin/spark-submit \
  --master spark://10.178.0.2:7077 \
  --conf spark.hadoop.google.cloud.auth.service.account.enable=true \
  --conf spark.hadoop.google.cloud.auth.service.account.json.keyfile=/home/jacob8753/keys/abiding-ascent-455400-u6-c8e90511db0d.json \
  --executor-memory 6G \
  --executor-cores 4 \
  /home/jacob8753/airflow2/dags/scripts/data_song200.py $DATE

echo "✅ [INFO] Spark job 완료 for $DATE"
