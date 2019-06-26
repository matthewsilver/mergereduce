cd /home/ubuntu/mergereduce/wikiextractor;
python WikiExtractor.py --json wikidata/enwiki-latest-pages-articles14.xml;
cd text; aws s3 cp . s3://mattsilver-insight/extracted --recursive
