import sys
import os
import re
import time
import configparser
import json
import redis
import mmh3
import numpy as np
import pickle
from termcolor import colored

from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import Tokenizer

from pyspark.sql.types import ArrayType, StringType, IntegerType
from pyspark.sql.functions import udf, concat, col, lit, explode, count, collect_list, size

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SQLContext

import nltk
nltk.download("wordnet")
from nltk.stem import WordNetLemmatizer

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config")
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib")
import config
import util

'''Master preprocessing script + General preprocessing functions'''

# Normalize (stem) words
def lemmatize(tokens):
    wordnet_lemmatizer = WordNetLemmatizer()
    stems = [wordnet_lemmatizer.lemmatize(token) for token in tokens]
    return stems

# Remove punctuation and spaces
def filter_body(body):
    remove_punctuation = re.sub(r"[^\w\s]", " ", body)
    remove_spaces = remove_punctuation.replace("\n", " ")
    return str(remove_spaces)


# Create n gram shingles from text body
def get_n_gram_shingles(tokens, n):
    return str([tokens[i:i+n] for i in range(len(tokens) - n + 1)])

def to_str(l):
    return str(l)
to_str_udf = udf(to_str, StringType())

# Preprocess the data and save to Redis
def preprocess_files(bucket_name, file_name):

    raw_data = sql_context.read.json("s3a://{0}/{1}".format(bucket_name, file_name))

    # Clean article text
    print(colored("[PROCESSING]: Cleaning article text", "green"))
    clean_body = udf(lambda body: filter_body(body), StringType())
    clean_article_data = raw_data.withColumn("cleaned_body", clean_body("text"))
    # Tokenize article text
    if (config.LOG_DEBUG): print(colored("[PROCESSING]: Tokenizing text vector...", "green"))
    tokenizer = Tokenizer(inputCol="cleaned_body", outputCol="text_body_tokenized")
    tokenized_data = tokenizer.transform(clean_article_data)
    
    # Remove stop words
    print(colored("[PROCESSING]: Removing stop words", "green"))
    stop_words_remover = StopWordsRemover(inputCol="text_body_tokenized", outputCol="text_body_stop_words_removed")
    stop_words_removed_data = stop_words_remover.transform(tokenized_data)
    
    # Stem words
    print(colored("Stemming tokenized text", "green"))
    stem = udf(lambda tokens: lemmatize(tokens), ArrayType(StringType()))
    stemmed_data = stop_words_removed_data.withColumn("text_body_stemmed", stem("text_body_stop_words_removed"))
    
    # Shingle resulting body
    print(colored("Shingling resulting text", "green"))
    shingle = udf(lambda tokens: get_n_gram_shingles(tokens, 3), StringType())
    shingled_data = stemmed_data.withColumn("text_body_shingled", shingle("text_body_stemmed"))
    shingle_table = shingled_data.select('id', 'text_body_shingled')
    
    if (config.LOG_DEBUG): print(colored("Adding category/id mappings to Redis", "green"))
 
    # Create a mapping of article categories to article id's that fall under that category. Each key is an article category and the values the list of article id's.
    cat_id_map = raw_data.select(explode('categories').alias('category'), 'id').groupBy(col('category')).agg(collect_list('id').alias('ids_list')).where(size(col('ids_list')) < 2000).withColumn('ids', to_str_udf('ids_list'))      
    print(colored("Beginning writing category/id mapping to Redis", "green")) 
    def write_cat_id_map_to_redis(rdd): 
        rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)
        for row in rdd:
            rdb.sadd('cat:{}'.format(row.category), row.ids) 
    cat_id_map.foreachPartition(write_cat_id_map_to_redis)
    print(cat_id_map.show(5,True))
    print(colored("Finished writing category/id mapping to Redis", "green"))

    #Minhash calculations
    k = 10
    random_seed = 50
    masks = (np.random.RandomState(seed=random_seed).randint(np.iinfo(np.int64).min, np.iinfo(np.int64).max, k))

    def update_min_hash_signature(word, min_hash_signature):
        root_hash = mmh3.hash64(word)[0]
        word_hashes = np.bitwise_xor(masks, root_hash)  # XOR root hash with k randomly generated integers to simulate k hash functions
        min_hash_signature = np.minimum(min_hash_signature, word_hashes)
        return min_hash_signature

    def calc_min_hash(tokens):
        min_hash_signature = np.empty(k, dtype=np.int64)
        min_hash_signature.fill(np.iinfo(np.int64).max)
        for token in tokens:
            min_hash_signature = update_min_hash_signature(token, min_hash_signature)
        return min_hash_signature

    def compute_minhash(df): 
       calc_min_hash = udf(lambda x: str(list(map(lambda x: int(x), calc_min_hash(x)))), StringType()) 
       df = df.withColumn("min_hash", calc_min_hash("text_body_shingled")).select('id', 'min_hash') 
       return df 
    print(colored("Computing minhash values", "green"))
    minhash_df = compute_minhash(shingle_table)
    print(colored("Finished computing minhash values", "green"))
    print(colored("Beginning writing minhash data to Redis", "green"))
    
    # Write minhash data to redis. If pipeline=True, use pipeline
    # method of inserting data in Redis
    def write_minhash_data_to_redis(rdd, pipeline=False):
        rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)
        if not pipeline:
            for row in rdd:
                rdb.sadd('id:{}'.format(row.id), row.min_hash)
            return
        else:
            i = 1
            pipe = rdb.pipeline()
            for row in rdd:
                pipe.set('id:{}'.format(row.id), row.min_hash)
                i += 1
                if i % 1000 == 0:
                    print('adding new batch. at {} entries total'.format(i))
                    pipe.execute()
                    pipe = rdb.pipeline()
    print(minhash_df.show(5, True)) 
    minhash_df.foreachPartition(write_minhash_data_to_redis)
    
    print(colored("Finished writing minhash data to Redis", "green"))
    
    if (config.LOG_DEBUG): print(colored("[UPLOAD]: Writing preprocessed data to database...", "green"))
#    write_aws_s3(config.S3_BUCKET, config.S3_FOLDER_PREPROCESSED, shingled_data)
    cf = configparser.ConfigParser()
    cf.read('../config/db_properties.ini')
    
def preprocess_all():
    bucket = util.get_bucket(config.S3_BUCKET)
    preprocess_files(config.S3_BUCKET, config.S3_FOLDER_EXTRACTED)

def main():
    #spark_conf = SparkConf().setAppName("Text Preprocesser").set("spark.cores.max", "30")

    global sc
    #sc = SparkContext(conf=spark_conf)
    sc_conf = SparkConf()
    sc_conf.set("spark.redis.host", config.REDIS_SERVER)
    sc_conf.set("spark.redis.port", "6379")
    sc = SparkContext(conf=sc_conf)
    sc.setLogLevel("ERROR")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib/util.py")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config/config.py")

    global sql_context
    sql_context = SQLContext(sc)

    start_time = time.time()
    preprocess_all()
    end_time = time.time()
    print(colored("Preprocessing run time (seconds): {0}".format(end_time - start_time), "magenta"))


if(__name__ == "__main__"):
    main()
