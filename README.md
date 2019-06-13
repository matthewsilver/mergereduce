# MergeReduce

### Overview
MergeReduce started out as a pipeline to ingest all English Wikipedia articles and determine which ones are similar enough in content to be merged (currently using the minhash algorithm). The scope of the project will probably change soon to be a more general pipeline to input custom text comparison algorithms and run them at scale, but for now the code is a rough pipeline of the original problem statement.

### Structure
At the first level of the directory, there are four scripts that run the pipeline:

_extract-wiki-data.sh_: take an unzipped "enwiki-latest-pages-articles" wikipedia file from the data dump (https://dumps.wikimedia.org/enwiki/latest/) and run WikiExtractor code to parse the text and categories out of the raw XML data. WikiExtractor code is at https://github.com/attardi/wikiextractor for reference; I added a few lines of code to automatically include the article category

_preprocess-wiki-data.sh_: run standard NLP preprocessing on output of extract-wiki-data.py, including tokenization, removal of stopwords and punctuation, and stemming (normalizing) words. Also breaks text into shingles, or n-grams, to prepare for minhash computation.

_calc-minhashes.sh_: calculate minhash values for text shingles

_calc-dedupe-candidates.sh_: calculate article pairs that may be potential duplicate candidates based on overlap of minhash values.

To see the Python program each script runs, look in the .sh file to see the file path the program redirects to.

### Notes

- In the src/batch_processing folder, there's code for computing MinHashLSH (a faster version of MinHash). Please ignore that for now! It may be implemented later but I didn't do it in time for this PR.
- Most code imports values from the config and/or utils files under src/config and src/lib, respectively.
- The WikiExtractor code is pretty extensive, and since I had to make some changes to the code I'm not just cloning the repo. Unfortunately this means a lot of code to look over but I'd recommend skimming at most.

### Planned Improvements

- Modularize current pipeline more so the entire function to compare two bodies of text is in one place (setting it up for the more generalized pipeline I'm pivoting my project towards)
- Hook things up to DB's like Postgres and Redis instead of writing to S3
- Build out docker/Kubernetes infrastructure to deploy a model at scale
