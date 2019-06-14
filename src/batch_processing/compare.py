import sys
import os
import time
import json
from termcolor import colored

from functools import reduce
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SQLContext, DataFrame
from pyspark.sql.functions import udf, col, size, explode, array_contains, count

from pyspark.sql.types import IntegerType, FloatType, ArrayType

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib")
import config
import util
import min_hash
import locality_sensitive_hash

find_lsh_sim = udf(lambda x, y: len(set(x) & set(y))/float(config.MIN_HASH_K_VALUE), FloatType())

def compute_minhash(df, mh):
    calc_min_hash = udf(lambda x: list(map(lambda x: int(x), mh.calc_min_hash_signature(x))), ArrayType(IntegerType()))
    df = df.withColumn("min_hash", calc_min_hash("text_body_shingled"))
    return df

intersect_size = udf(lambda c1, c2: len(set(c1) & set(c2)), IntegerType())

def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs)

# Find duplicate candidates
def find_dup_cands_within_tags(df, mh, threshold=0.9):
    all_dfs = []
    all_cats = df.select(explode(col('categories')).alias('cat')).groupBy(col('cat')).agg(count('*').alias('num_entries')).where(col('num_entries') > 5).collect()
    num_cats = len(all_cats)
    print('found {} categories total'.format(num_cats))
    i = 1
    threshold = 0.1
    for cat in all_cats:
        cat = str(cat["cat"]).strip()
        print('looking at category: {}. {} of {} categories ({}%) complete'.format(cat, i, num_cats, 100*round(1.0*i/num_cats, 2)))
        df_filtered = df.where(array_contains(col('categories'), cat)).cache()
        minhash_df = df_filtered.alias("q1").join(df.alias("q2"), col("q1.id") < col("q2.id")).select(
        col("q1.id").alias("q1_id"),
        col("q2.id").alias("q2_id"),
        col("q1.min_hash").alias("q1_min_hash"),
        col("q2.min_hash").alias("q2_min_hash"),
        col("q1.title").alias("q1_title"),
        col("q2.title").alias("q2_title"),
        find_lsh_sim("q1.min_hash", "q2.min_hash").alias("min_hash_overlap")
            ).where(col('min_hash_overlap') >= threshold)
        i += 1
        print(minhash_df.show())
        minhash_df_count = minhash_df.count()
        if minhash_df_count > 0:
            print('writing {} potential dupes in category to file'.format(minhash_df_count))
            util.write_aws_s3(config.S3_BUCKET_BATCH_PREPROCESSED, config.S3_FOLDER_BATCH_RAW+'/potential_dupes/partition={}'.format(cat), minhash_df)
    #return unionAll(*all_dfs)

def run_minhash():
    df = util.read_all_json_from_bucket_folder(sql_context, config.S3_BUCKET, config.S3_FOLDER_PREPROCESSED)
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
    return minhash_df
    # Compute pairwise LSH similarities for questions within tags
    if (config.LOG_DEBUG): print(colored("[BATCH]: Fetching questions in same tag, comparing LSH and MinHash, uploading duplicate candidates back to Redis...", "cyan"))
    #potential_dupes_df = find_dup_cands_within_tags(minhash_df, mh, 0.9)
    #return potential_dupes_df    

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
    minhash_df = run_minhash()
    util.write_aws_s3(config.S3_BUCKET, config.S3_FOLDER_MINHASHES, minhash_df)
    #potential_dupes_df = run_minhash()
    #util.write_aws_s3(config.S3_BUCKET_BATCH_PREPROCESSED, config.S3_FOLDER_BATCH_RAW+'/potential_dupes', potential_dupes_df)
    end_time = time.time()
    print(colored("Spark MinHash run time (seconds): {0} seconds".format(end_time - start_time), "magenta"))


if(__name__ == "__main__"):
    main()
