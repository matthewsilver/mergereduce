import os
import sys
import time
import configparser
import numpy as np
import mmh3
import itertools
import pickle
import redis
import psycopg2
from termcolor import colored

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf, col

from pyspark.sql.types import IntegerType, FloatType, ArrayType

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config")
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib")
import config
import util

def compare_text():

    calc_overlap = udf(lambda x, y: 1.0*len(set(x) & set(y))/k, FloatType())
    
    cf = configparser.ConfigParser()
    cf.read('../config/db_properties.ini')
    if (config.LOG_DEBUG): print(colored("[UPLOAD]: Reaing shingle data from database...", "green"))
    #df = sql_context.read.jdbc(cf['postgres']['url_preprocess'], table='shingle_data', properties={'user': cf['postgres']['user'], 'password': cf['postgres']['password']})    
    #print('there are {} articles\n================='.format(df.count()))
    
    # Compute MinHash for every article
    print(colored("[BATCH]: Calculating MinHash hashes and LSH hashes...", "green"))
    #minhash_df = compute_minhash(df)
   
    print(colored("Writing results to database", "green"))    
    connection = psycopg2.connect(host=cf['postgres']['url_results'], database='similarity_scores', user=cf['postgres']['user'], password=cf['postgres']['password'])
    cursor = connection.cursor()    
 
    rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)
    for category in rdb.scan_iter('cat:*'):
        pairs = list(itertools.combinations(eval(list(rdb.smembers(category))[0]), 2))
        print("Evaluating potential for {} pairs in category {}".format(len(pairs), category))
        for pair in pairs:
           minhash1 = rdb.mget(pair[0])
           minhash2 = rdb.mget(pair[1])
           if minhash1 and minhash2 and minhash1[0] is not None and minhash2[0] is not None:
               minhash1 = eval(list(minhash1))
               minhash2 = eval(list(minhash2))
               overlap = 1.0 * set(minhash1) & set(minhash2)/len(minhash1)
               print(pair[0], pair[1], overlap)
               if overlap > 0.9:
                   print(pair[0], pair[1], overlap)
                   cursor.execute('''INSERT INTO scores (id1, id2, score) VALUES (%s, %s, %s)''', (pair[0], pair[1], overlap))           
    #similarity_scores_df = minhash_df.alias('q1').join(
    #minhash_df.alias('q2'), col('q1.id') < col('q2.id')
    #).select(
    #col('q1.url').alias('q1_url'),
    #col('q2.url').alias('q2_url'),
    #calc_overlap('q1.min_hash', 'q2.min_hash').alias('lsh_sim')
    #)
    
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
