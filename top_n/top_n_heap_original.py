#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
get the top n words after word count

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

TOP_N = 3

conf = SparkConf().setAppName("TopN").setMaster("local[2]")
sc = SparkContext(conf=conf)


def word_count(rdd):
    pre_process_rdd = rdd.map(lambda line: line.strip("!,."))
    words_rdd = pre_process_rdd.flatMap(lambda line: line.split())
    pairs_rdd = words_rdd.map(lambda word: (word, 1))
    count_rdd = pairs_rdd.reduceByKey(lambda x, y: x+y)
    return count_rdd


def top_n(iterator):
    """
    top n of one partition
    :param iterator:
    :return:
    """
    heap = []
    for item in iterator:
        print(item)
        # if length of heap is longer than TOP_N, replace instead of push
        if len(heap) <= TOP_N:
            heapq.heappush(heap, item)
        else:
            heapq.heapreplace(heap, item)

    # get k largest element from heap
    res = heapq.nlargest(TOP_N, heap, key=get_count)
    return res


def get_count(w_count):
    return w_count[1]


def merge(topn_rdd):
    """
    merge result from different partitions
    :param topn_rdd:
    :return:
    """
    res = topn_rdd.sortBy(lambda x: x[1], ascending=False)\
                  .zipWithIndex()\
                  .filter(lambda x: x[1] < TOP_N)\
                  .keys()
    return res


# todo: df
if __name__ == "__main__":
    text_rdd = sc.textFile("../data/word_count.txt", 4)
    print("partitions:", text_rdd.getNumPartitions())
    counts = word_count(text_rdd)
    print(counts.take(10))
    # calculate the top n of each partition
    topn_partitions = counts.mapPartitions(top_n)
    topn = merge(topn_partitions)
    print(topn.collect())
