cd /home/ubuntu/mergereduce/src/preprocess;
export PYSPARK_PYTHON=/usr/bin/python3;
spark-submit --master spark://ip-10-0-0-7:7077 --conf spark.executor.memoryOverhead=3000  --executor-memory 6G --driver-memory 6G preprocess.py
