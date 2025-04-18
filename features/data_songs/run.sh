#!/bin/bash

SPARK_HOME=/home/joon/app/spark-3.5.1-bin-hadoop3
DS=$1
PY_PATH=$2

cd /home/joon/code/WeatherTunes
source .venv/bin/activate

$SPARK_HOME/bin/spark-submit \
--master spark://spark-jerry-1.asia-northeast3-c.c.wiki-455500.internal:7077 \
--executor-memory 2G \
--executor-cores 2 \
$PY_PATH $DS
