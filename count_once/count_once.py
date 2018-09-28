#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
there is an array, and only one element occurs one time,
others occurs 2 times, find it.
"""
from __future__ import print_function
import os

from pyspark import SparkConf
from pyspark import SparkContext

os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
os.environ['PYSPARK_SUBMIT_ARGS'] = \
    '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.1 ' \
    'pyspark-shell'

conf = SparkConf().setAppName("count_once").setMaster("local[2]")
sc = SparkContext(conf=conf)


def main():
    block_ids = raw_rdd.map(lambda line: line.strip(",")).flatMap(
        lambda line: line.split(","))
    print(block_ids.collect())
    # xor operation,
    # the id occurs two time will result in 0, one time is itself
    res = block_ids.map(lambda x: int(x)).reduce(lambda x, y: x ^ y)
    print(res)


if __name__ == "__main__":
    # read block ids from file
    raw_rdd = sc.textFile("../data/count_once.txt")
    main()

