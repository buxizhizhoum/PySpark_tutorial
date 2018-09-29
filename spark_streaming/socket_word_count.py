#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
word count with spark socket streaming
"""
from __future__ import print_function
import os

from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
os.environ['PYSPARK_SUBMIT_ARGS'] = \
    '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.1 ' \
    'pyspark-shell'

spark = SparkSession\
    .builder\
    .appName("word_count")\
    .master("local[2]")\
    .getOrCreate()

sc = spark.sparkContext
ssc = StreamingContext(sparkContext=sc, batchDuration=1)

lines = ssc.socketTextStream("localhost", 9999)


def main():
    words = lines.flatMap(lambda line: line.split(" "))
    pairs = words.map(lambda word: (word, 1))
    word_counts = pairs.reduceByKey(lambda x, y: x + y)

    word_counts.pprint()

    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()



