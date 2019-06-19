cd /home/ubuntu/mergereduce/src/preprocess;
spark-submit --master spark://ip-10-0-0-4:7077 --conf spark.executor.memoryOverhead=600  --executor-memory 6G --driver-memory 6G preprocess.py
