import sys
import os
from pyspark.sql.types import FloatType
from pyspark.sql.functions import col, udf

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SQLContext, DataFrame

global sc
spark_conf = SparkConf()
sc = SparkContext(conf=spark_conf)
sc.setLogLevel("ERROR")
sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib/min_hash.py")
sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib/locality_sensitive_hash.py")
sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib/util.py")
sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config/config.py")

global sql_context
sql_context = SQLContext(sc)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib")

import config
import util
import min_hash
import locality_sensitive_hash

def common_bands_count(a, b):
  return len(set(a) & set(b))

find_lsh_sim = udf(lambda x, y: round(1.0*common_bands_count(x,y)/float(config.MIN_HASH_K_VALUE), 3), FloatType())

df = sql_context.read.json('s3a://mattsilver-insight/minhash')

lsh_sim_df = df.alias('q1').join(df.alias('q2'), col('q1.id') < col('q2.id')).select(col('q1.url').alias('q1_url'), col('q2.url').alias('q2_url'), find_lsh_sim('q1.min_hash', 'q2.min_hash').alias('lsh_sim'))

#lsh_sim_df.orderBy(col('lsh_sim').desc())

util.write_aws_s3(config.S3_BUCKET, config.S3_FOLDER_OUTPUT, lsh_sim_df)
