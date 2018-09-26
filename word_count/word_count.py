#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function
import os

from pyspark import SparkContext
from pyspark import SparkConf


os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
os.environ['PYSPARK_SUBMIT_ARGS'] = \
    '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.1 ' \
    'pyspark-shell'

conf = SparkConf().setAppName("WordCount").setMaster("local[2]")
sc = SparkContext(conf=conf)


def main():
    text_rdd = sc.textFile("../data/word_count.txt")
    print("text:", text_rdd.collect())

    # remove comma, dot at the end of line
    preprocess_rdd = text_rdd.map(lambda line: line.rstrip("!,."))
    print("preprocess:", preprocess_rdd.collect())

    words_rdd = preprocess_rdd.flatMap(lambda line: line.split())
    print("words_rdd:", words_rdd.collect())

    pairs_rdd = words_rdd.map(lambda word: (word, 1))
    print("pairs_rdd:", pairs_rdd.collect())

    count_rdd = pairs_rdd.reduceByKey(lambda x, y: x + y)
    print("count_rdd:", count_rdd.collect())


if __name__ == "__main__":
    main()
