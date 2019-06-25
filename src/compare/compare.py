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
#sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib")
import config
#import util

def compare_text():

    cf = configparser.ConfigParser()
    cf.read('../config/db_properties.ini')
    print(colored("Reading shingle data from redis", "green"))
    
    print(colored("Calculating minhash value overlap for articles in each category and writing results out to database", "green"))
  
    # Set up postgres connection for writing similarity scores 
    connection = psycopg2.connect(host=cf['postgres']['url_results'], database='similarity_scores', user=cf['postgres']['user'], password=cf['postgres']['password'])
    cursor = connection.cursor()    

    # Set up redis connection for reading in minhash values
    rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)

    # For each category, go through each pair of articles and write the ones with a high enough minhash overlap to a database
    URL_HEADER = 'https://www.wikipedia.org/wiki?curid='
    for category in rdb.scan_iter('cat:*'):
        pairs = list(itertools.combinations(eval(list(rdb.smembers(category))[0]), 2))
        print("Evaluating potential for {} pairs in category {}".format(len(pairs), category))
        for pair in pairs:
           minhash1 = rdb.smembers('id:{}'.format(pair[0]))
           minhash2 = rdb.smembers('id:{}'.format(pair[1]))
           if minhash1 and minhash2:
               minhash1 = eval(list(minhash1)[0])
               minhash2 = eval(list(minhash2)[0])
               overlap = 1.0 * len(set(minhash1) & set(minhash2))/len(minhash1)
               if overlap > 0.0:
                   url1 = URL_HEADER + pair[0]
                   url2 = URL_HEADER + pair[1]
                   #print(category, url1, url2, overlap)
                   cursor.execute('''INSERT INTO scores (id1, id2, score, category) VALUES (%s, %s, %s, %s)''', (url1, url2, overlap, str(category)))
                   connection.commit() 
    
def main():
    spark_conf = SparkConf().setAppName("Spark Custom MinHashLSH").set("spark.cores.max", "30")

    global sc
    global sql_context    

    sc = SparkContext(conf=spark_conf)
    sc.setLogLevel("ERROR")
    sql_context = SQLContext(sc)
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
