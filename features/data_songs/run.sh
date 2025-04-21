#!/bin/bash

SPARK_HOME=/home/wsl/app/spark-3.5.1-bin-hadoop3
DS=$1
PY_PATH=$2

cd /home/wsl/code/WeatherTunes
source .venv/bin/activate

$SPARK_HOME/bin/spark-submit \
  --master local[*] \
  $PY_PATH $DS
