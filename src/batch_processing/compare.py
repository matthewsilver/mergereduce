import os
import time
import configparser
import numpy as np
import mmh3
import pickle
from termcolor import colored

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf, col

from pyspark.sql.types import IntegerType, FloatType, ArrayType

def compare_text():

    k = 5
    random_seed = 50
    masks = (np.random.RandomState(seed=random_seed).randint(np.iinfo(np.int64).min, np.iinfo(np.int64).max, k))

    def update_min_hash_signature(word, min_hash_signature):
        #root_hash = mmh3.hash64(word.encode("ascii", "ignore"))[0]
        root_hash = mmh3.hash64(pickle.dumps(word))[0]  # For MinHashing shingles
        word_hashes = np.bitwise_xor(masks, root_hash)  # XOR root hash with k randomly generated integers to simulate k hash functions, can add bitroll if there's time
        min_hash_signature = np.minimum(min_hash_signature, word_hashes)
        return min_hash_signature

    def calc_min_hash_signature(tokens):
        min_hash_signature = np.empty(k, dtype=np.int64)
        min_hash_signature.fill(np.iinfo(np.int64).max)
        for token in tokens:
            min_hash_signature = update_min_hash_signature(token, min_hash_signature)
        return min_hash_signature    

    calc_overlap = udf(lambda x, y: 1.0*len(set(x) & set(y))/k, FloatType())
    
    def compute_minhash(df):
       calc_min_hash = udf(lambda x: list(map(lambda x: int(x), calc_min_hash_signature(x))), ArrayType(IntegerType()))
       df = df.withColumn("min_hash", calc_min_hash("text_body_shingled"))
       return df
    df = sql_context.read.json("s3a://mattsilver-insight/preprocessed").limit(100)
    print('there are {} articles\n================='.format(df.count()))
    
    # Compute MinHash for every article
    print(colored("[BATCH]: Calculating MinHash hashes and LSH hashes...", "green"))
    minhash_df = compute_minhash(df)
    
    
    similarity_scores_df = minhash_df.alias('q1').join(
    minhash_df.alias('q2'), col('q1.id') < col('q2.id')
    ).select(
    col('q1.url').alias('q1_url'),
    col('q2.url').alias('q2_url'),
    calc_overlap('q1.min_hash', 'q2.min_hash').alias('lsh_sim')
    )
    
    return similarity_scores_df

def main():
    spark_conf = SparkConf().setAppName("Spark Custom MinHashLSH").set("spark.cores.max", "30")

    global sc
    sc = SparkContext(conf=spark_conf)
    sc.setLogLevel("ERROR")
    global sql_context
    sql_context = SQLContext(sc)

    sc.setLogLevel("ERROR")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib/util.py")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config/config.py")


    start_time = time.time()
    similarity_scores_df = compare_text()

    config = configparser.ConfigParser()
    config.read('../config/db_properties.ini')
    similarity_scores_df.write.jdbc(config['postgres']['url'], config['postgres']['table'], mode='overwrite', properties={'user': config['postgres']['user'], 'password': config['postgres']['password']})    

    end_time = time.time()
    print(colored("Spark MinHash run time (seconds): {0} seconds".format(end_time - start_time), "magenta"))


if(__name__ == "__main__"):
    main()
