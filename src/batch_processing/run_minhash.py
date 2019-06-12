import sys
import os
import time
import json
from termcolor import colored

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf, col, size

from pyspark.sql.types import IntegerType, FloatType, ArrayType

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib")
import config
import util
import min_hash

find_lsh_sim = udf(lambda x, y: lsh.common_bands_count(x,y)/config.MIN_HASH_K_VALUE, FloatType())

def compute_minhash(df, mh):
    calc_min_hash = udf(lambda x: list(map(lambda x: int(x), mh.calc_min_hash_signature(x))), ArrayType(IntegerType()))
    df = df.withColumn("min_hash", calc_min_hash("text_body_shingled"))
    return df

intersect_size = udf(lambda c1, c2: len(set(c1) & set(c2)), IntegerType())

# Find duplicate candidates
def find_dup_cands_within_tags(df, mh, threshold=0.9):
    minhash_df = df.alias("q1").join(df.alias("q2"), intersect_size(col("q1.min_hash"), col("q2.min_hash")) > 2).select(
    col("q1.id").alias("q1_id"),
    col("q2.id").alias("q2_id"),
    col("q1.min_hash").alias("q1_min_hash"),
    col("q2.min_hash").alias("q2_min_hash"),
    col("q1.title").alias("q1_title"),
    col("q2.title").alias("q2_title"),
    find_lsh_sim("q1.min_hash", "q2.min_hash").alias("min_hash_overlap")
            ).where(col('min_hash_overlap') >= threshold)

    return minhash_df

def run_minhash():
    df = util.read_all_json_from_bucket_folder(sql_context, config.S3_BUCKET_BATCH_PREPROCESSED, 'text/output')
    print('there are {} articles\n================='.format(df.count()))
    #  Create and save MinHash and LSH if not exist or load them from file
    if(not os.path.isfile(config.MIN_HASH_PICKLE) or not os.path.isfile(config.LSH_PICKLE)):
        mh = min_hash.MinHash(config.MIN_HASH_K_VALUE)
        util.save_pickle_file(mh, config.MIN_HASH_PICKLE)
    else:
        mh = util.load_pickle_file(config.MIN_HASH_PICKLE)

    # Compute MinHash for every article
    if (config.LOG_DEBUG): print(colored("[BATCH]: Calculating MinHash hashes and LSH hashes...", "green"))
    minhash_df = compute_minhash(df, mh)

    # Compute pairwise LSH similarities for questions within tags
    if (config.LOG_DEBUG): print(colored("[BATCH]: Fetching questions in same tag, comparing LSH and MinHash, uploading duplicate candidates back to Redis...", "cyan"))
    potential_dupes_df = find_dup_cands_within_tags(minhash_df, mh, 0.9)
    return potential_dupes_df    

def main():
    spark_conf = SparkConf().setAppName("Spark Custom MinHashLSH").set("spark.cores.max", "30")

    global sc
    sc = SparkContext(conf=spark_conf)
    sc.setLogLevel("ERROR")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib/min_hash.py")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib/locality_sensitive_hash.py")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib/util.py")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config/config.py")

    global sql_context
    sql_context = SQLContext(sc)

    start_time = time.time()
    potential_dupes_df = run_minhash()
    util.write_aws_s3(config.S3_BUCKET_BATCH_PREPROCESSED, config.S3_FOLDER_BATCH_RAW+'/potential_dupes', potential_dupes_df)
    end_time = time.time()
    print(colored("Spark MinHash run time (seconds): {0} seconds".format(end_time - start_time), "magenta"))


if(__name__ == "__main__"):
    main()
