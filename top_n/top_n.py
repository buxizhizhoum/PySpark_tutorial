#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
get the top n words after word count

refer: https://stackoverflow.com/a/34293270/8025086
"""
from __future__ import print_function
import os

from pyspark import SparkContext
from pyspark import SparkConf


os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
os.environ['PYSPARK_SUBMIT_ARGS'] = \
    '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.1 ' \
    'pyspark-shell'

TOP_N = 3

conf = SparkConf().setAppName("TopK").setMaster("local[2]")
sc = SparkContext(conf=conf)


def word_count(rdd):
    pre_process_rdd = rdd.map(lambda line: line.strip("!,."))
    words_rdd = pre_process_rdd.flatMap(lambda line: line.split())
    pairs_rdd = words_rdd.map(lambda word: (word, 1))
    count_rdd = pairs_rdd.reduceByKey(lambda x, y: x+y)
    return count_rdd


def main():
    counts = word_count(text_rdd)
    print(counts.take(10))
    # get the top n high frequency words
    topn = counts.top(TOP_N, key=lambda x: x[1])
    print(topn)


if __name__ == "__main__":
    text_rdd = sc.textFile("../data/word_count.txt")
    main()


