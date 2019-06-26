cd /home/ubuntu/mergereduce/src/preprocess;
export PYSPARK_PYTHON=/usr/bin/python3;
spark-submit --master spark://$1 --conf spark.executor.memoryOverhead=600  --executor-memory 6G --driver-memory 6G preprocess.py
#spark-submit preprocess.py
