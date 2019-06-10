from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext()
sc.setLogLevel("ERROR")

def main():
    #A bunch of code is outputted when starting Pyspark so use separator
    print("="*30)
    sql_context = SQLContext(sc)
    filepath = "/home/ubuntu/wikiextractor/text/*/*"
    print("Reading processed Wiki JSON files")
    df = sql_context.read.json(filepath)
    print("Finished reading")
    print("Number of rows in dataframe is {}".format(df.count()))

if (__name__ == "__main__"):
    main()
