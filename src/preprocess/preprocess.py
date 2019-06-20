import sys
import os
import re
import time
import configparser
import json
import redis
from termcolor import colored

from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import Tokenizer

from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import udf, concat, col, lit, explode, count, collect_list, size

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SQLContext

import nltk
nltk.download("wordnet")
from nltk.stem import WordNetLemmatizer

#reload(sys)
#sys.setdefaultencoding('utf-8')

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config")
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib")
import config
import util

'''Master preprocessing script + General preprocessing functions'''

# Stems words
def lemmatize(tokens):
    wordnet_lemmatizer = WordNetLemmatizer()
    stems = [wordnet_lemmatizer.lemmatize(token) for token in tokens if len(token) > 1]
    return tuple(stems)


# Removes code snippets and other irregular sections from question body, returns cleaned string
def filter_body(body):
#    remove_code = re.sub('<[^>]+>', '', body)
    remove_punctuation = re.sub(r"[^\w\s]", " ", body)
    remove_spaces = remove_punctuation.replace("\n", " ")
    return remove_spaces.encode('ascii', 'ignore')


# Create 2 gram shingles from text body
def get_n_gram_shingles(tokens, n):
    return str([tuple(tokens[i:i+n]) for i in range(len(tokens) - n + 1)])


# Preprocess a data file and upload it
def preprocess_file(bucket_name, file_name):

    raw_data = sql_context.read.json("s3a://{0}/{1}".format(bucket_name, file_name))

    # Clean article text
    if(config.LOG_DEBUG): print(colored("[PROCESSING]: Cleaning article text", "green"))
    clean_body = udf(lambda body: filter_body(body), StringType())
    partially_cleaned_data = raw_data.withColumn("cleaned_body", clean_body("text"))

    # Tokenize article text
    if (config.LOG_DEBUG): print(colored("[PROCESSING]: Tokenizing text vector...", "green"))
    tokenizer = Tokenizer(inputCol="cleaned_body", outputCol="text_body_tokenized")
    tokenized_data = tokenizer.transform(partially_cleaned_data)

    # Remove stop words
    if (config.LOG_DEBUG): print(colored("[PROCESSING]: Removing stop words...", "green"))
    stop_words_remover = StopWordsRemover(inputCol="text_body_tokenized", outputCol="text_body_stop_words_removed")
    stop_words_removed_data = stop_words_remover.transform(tokenized_data)

    # Stem words
    if (config.LOG_DEBUG): print(colored("[PROCESSING]: Stemming tokenized vector...", "green"))
    stem = udf(lambda tokens: lemmatize(tokens), ArrayType(StringType()))
    stemmed_data = stop_words_removed_data.withColumn("text_body_stemmed", stem("text_body_stop_words_removed"))

    # Shingle resulting body
    if (config.LOG_DEBUG): print(colored("[PROCESSING] Shingling resulting text body...", "green"))
    shingle = udf(lambda tokens: get_n_gram_shingles(tokens, 3), StringType())
    shingled_data = stemmed_data.withColumn("text_body_shingled", shingle("text_body_stemmed"))
    shingle_table = shingled_data.select('id', 'text_body_shingled')
    # Write to AWS
#    print('process {} articles total'.format(shingled_data.count()))
   
    if (config.LOG_DEBUG): print(colored("Adding category/id mappings to Redis", "green"))
    
    def list_to_str(l):
        return str(l)
    list_to_str_udf = udf(list_to_str, StringType())
    
    cat_id_map = raw_data.select(explode('categories').alias('category'), 'id').groupBy(col('category')).agg(collect_list('id').alias('ids_list')).where(size(col('ids_list')) < 2000).withColumn('ids', list_to_str_udf('ids_list'))      
    
    def write_cat_id_map_to_redis(rdd): 
        rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)
        for row in rdd:
            rdb.sadd(row.category, row.ids) 
    cat_id_map.foreachPartition(write_cat_id_map_to_redis)
    print(colored("Finished writing category/id maping to Redis", "green"))
    exit(0)
    ids_to_compare = ids1.join(
        ids2,
        (ids1.cat == ids2.cat) & (ids1.id1 < ids2.id2 )).dropDuplicates(['id1', 'id2']).select('id1', 'id2')
    print('{} total ids to compare'.format(ids_to_compare.count())) 
    def write_aws_s3(bucket_name, file_name, df):
        df.write.save("s3a://{0}/{1}".format(bucket_name, file_name), format="json", mode="overwrite")

    if (config.LOG_DEBUG): print(colored("[UPLOAD]: Writing preprocessed data to database...", "green"))
#    write_aws_s3(config.S3_BUCKET, config.S3_FOLDER_PREPROCESSED, shingled_data)
    cf = configparser.ConfigParser()
    cf.read('../config/db_properties.ini')
    if (config.LOG_DEBUG): print(colored("[UPLOAD]: Writing shingle data to database...", "green")) 
    #shingle_table.write.jdbc(cf['postgres']['url_preprocess'], 'shingle_data', mode='overwrite', properties={'user': cf['postgres']['user'], 'password': cf['postgres']['password']}) 
    if (config.LOG_DEBUG): print(colored("[UPLOAD]: Writing id comparisons to database...", "green")) 
    ids_to_compare.write.jdbc(cf['postgres']['url_preprocess'], 'ids_to_compare', mode='overwrite', properties={'user': cf['postgres']['user'], 'password': cf['postgres']['password']})

 
def preprocess_all():
    bucket = util.get_bucket(config.S3_BUCKET)
    preprocess_file(config.S3_BUCKET, config.S3_FOLDER_EXTRACTED)
    #for csv_obj in bucket.objects.all():
    #    preprocess_file(config.S3_BUCKET_BATCH_RAW, csv_obj.key)
    #    print(colored("Finished preprocessing file s3a://{0}/{1}".format(config.S3_BUCKET_BATCH_RAW, csv_obj.key), "green"))


def main():
    #spark_conf = SparkConf().setAppName("Text Preprocesser").set("spark.cores.max", "30")

    global sc
    #sc = SparkContext(conf=spark_conf)
    sc = SparkContext()
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
