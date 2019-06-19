import os

# Program settings
SRC_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DEBUG = True


# Preprocessing settings
NUM_SYNONYMS = 0  # num closest synonyms to add to token vector


# AWS settings
S3_BUCKET = "mattsilver-insight"
S3_FOLDER_RAW = "raw"
S3_FOLDER_PREPROCESSED = "preprocessed"
S3_FOLDER_EXTRACTED = "extracted_flat"
S3_FOLDER_MINHASHES = "minhash"
S3_FOLDER_OUTPUT = "dedupe_recs"

# Kafka settings
KAFKA_SERVERS = ""  # ip:port for kafka brokers
KAFKA_PRODUCER_RATE = 0  # seconds
KAFKA_TOPIC = ""
KAFKA_CONSUMER_REFRESH = 0  # seconds


# Spark settings - streaming, batch
SPARK_STREAMING_MINI_BATCH_WINDOW = 0  # seconds


# Redis settings
REDIS_SERVER = "redis-cluster-two.ez7sgh.0001.use1.cache.amazonaws.com"


# MinHash, LSH parameters
MIN_HASH_K_VALUE = 150
LSH_NUM_BANDS = 30
LSH_BAND_WIDTH = 5
LSH_NUM_BUCKETS = 90
LSH_SIMILARITY_BAND_COUNT = 1  # Number of common bands needed for MinHash comparison


# Dump files to synchronize models across spark streaming/batch
MIN_HASH_PICKLE = SRC_PATH + "/lib/mh.pickle"
LSH_PICKLE = SRC_PATH + "/lib/lsh.pickle"


# Duplicate question settings
DUP_QUESTION_MIN_HASH_THRESHOLD = 0
DUP_QUESTION_MIN_TAG_SIZE = 0
DUP_QUESTION_IDENTIFY_THRESHOLD = 0
QUESTION_POPULARITY_THRESHOLD = 0
