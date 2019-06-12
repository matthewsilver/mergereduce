import sys
import os
import re
import time
from termcolor import colored

from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import Tokenizer

from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import udf, concat, col, lit

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
    return [tuple(tokens[i:i+n]) for i in range(len(tokens) - n + 1)]


# Preprocess a data file and upload it
def preprocess_file(bucket_name, file_name):

    raw_data = sql_context.read.json("s3a://{0}/{1}".format(bucket_name, file_name + '/AA/wiki_0*'))

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
    shingle = udf(lambda tokens: get_n_gram_shingles(tokens, 3), ArrayType(ArrayType(StringType())))
    shingled_data = stemmed_data.withColumn("text_body_shingled", shingle("text_body_stemmed"))

    # Write to AWS
    print('process {} articles total'.format(shingled_data.count()))
    if (config.LOG_DEBUG): print(colored("[UPLOAD]: Writing preprocessed data to AWS...", "green"))
    util.write_aws_s3(config.S3_BUCKET, config.S3_FOLDER_PREPROCESSED, shingled_data, "json")


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
