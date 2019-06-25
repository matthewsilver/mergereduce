cd /home/ubuntu/mergereduce/src/compare;
export PYSPARK_PYTHON=/usr/bin/python3;
spark-submit --master spark://$1 --conf spark.executor.memoryOverhead=3000  --executor-memory 6G --driver-memory 6G compare.py
