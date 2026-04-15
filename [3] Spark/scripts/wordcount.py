from pyspark import SparkContext, SparkConf
import time

conf = SparkConf().setAppName("Bai1_WordCount_RDD").setMaster("spark://spark-master:7077")
sc = SparkContext(conf=conf)

text_file = sc.textFile("file:///opt/bitnami/spark/data/sample.txt")

counts = text_file.flatMap(lambda line: line.split(" ")) \
                  .map(lambda word: (word, 1)) \
                  .reduceByKey(lambda a, b: a + b)

for word, count in counts.collect():
    print(f"Từ '{word}': {count}")

try:
    time.sleep(600)
except KeyboardInterrupt:
    pass

sc.stop()