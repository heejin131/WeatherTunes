#!/bin/bash

SPARK_HOME=/home/wsl/app/spark-3.5.1-bin-hadoop3
DS=$1
PY_PATH=$2

cd /home/wsl/code/WeatherTunes
source .venv/bin/activate

SCRIPT_PATH=$(realpath $PY_PATH)

$SPARK_HOME/bin/spark-submit \
  --master local[*] \
  --conf spark.ui.port=8899 \
  $SCRIPT_PATH $DS