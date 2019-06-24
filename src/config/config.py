S3_BUCKET = "mattsilver-insight"
S3_FOLDER_RAW = "raw"
S3_FOLDER_PREPROCESSED = "preprocessed"
S3_FOLDER_EXTRACTED = "extracted_flat"
S3_FOLDER_MINHASHES = "minhash"
S3_FOLDER_OUTPUT = "dedupe_recs"

REDIS_SERVER = "ec2-3-220-51-118.compute-1.amazonaws.com"

MIN_HASH_K_VALUE = 150

# Paths for pickled models
MIN_HASH_PICKLE = "../lib/mh.pickle"
LSH_PICKLE = "../lib/lsh.pickle"
