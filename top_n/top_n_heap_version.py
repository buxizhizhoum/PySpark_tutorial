#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
get the top n words after word count with heap

refer: https://stackoverflow.com/a/34293270/8025086
"""
from __future__ import print_function
import heapq
import os

from pyspark import SparkContext
from pyspark import SparkConf


os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
os.environ['PYSPARK_SUBMIT_ARGS'] = \
    '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.1 ' \
    'pyspark-shell'

TOP_N = 2

conf = SparkConf().setAppName("TopK").setMaster("local[2]")
sc = SparkContext(conf=conf)


def word_count(rdd):
    pre_process_rdd = rdd.map(lambda line: line.strip("!,."))
    words_rdd = pre_process_rdd.flatMap(lambda line: line.split())
    pairs_rdd = words_rdd.map(lambda word: (word, 1))
    count_rdd = pairs_rdd.reduceByKey(lambda x, y: x+y)
    return count_rdd


def top_n(iterator):
    """
    top n of a partition
    :param iterator:
    :return:
    """
    res = heapq.nlargest(TOP_N, iterator, key=get_count)
    return res


def get_count(w_count):
    return w_count[1]


if __name__ == "__main__":
    text_rdd = sc.textFile("../data/word_count.txt", 4)
    counts = word_count(text_rdd)
    print(counts.take(10))
    # sortBy sort the rdd after get top n of each partition
    # zipWithIndex add index after sort
    # filter will limit the number of element
    # keys will remove index which is added by zipWithIndex
    topk = counts.mapPartitions(top_n)\
                 .sortBy(lambda x: x[1], ascending=False)\
                 .zipWithIndex()\
                 .filter(lambda x: x[1] < TOP_N)\
                 .keys()
    print(topk.collect())
