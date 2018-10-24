#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
write to redis foreachPartition
refer: http://spark.apache.org/docs/latest/streaming-programming-guide.html#design-patterns-for-using-foreachrdd
"""
from __future__ import print_function
import redis
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


def write_redis(partition):
    """
    write redis of a rdd by create a connection for each partition
    :param partition:
    :return:
    """
    print("partition:", partition)
    r = redis.StrictRedis(host='localhost', port=6379)

    for item in partition:
        print("rdd_data", item)
        r.sadd("test", item)


def main():
    words = lines.flatMap(lambda line: line.split(" "))
    pairs = words.map(lambda word: (word, 1))
    word_counts = pairs.reduceByKey(lambda x, y: x + y)

    word_counts.foreachRDD(lambda rdd: rdd.foreachPartition(write_redis))


if __name__ == "__main__":
    main()


