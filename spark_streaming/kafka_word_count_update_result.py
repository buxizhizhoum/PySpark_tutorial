#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
word count with kafka and save result to redis
consume data from kafka topic "test" and do word count with batch duration of 1 second

the result saved in redis is updated when new data is consumed form kafka.
"""
from __future__ import print_function
import os

import redis

from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
os.environ['PYSPARK_SUBMIT_ARGS'] = \
    '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.1 ' \
    'pyspark-shell'

spark = SparkSession\
    .builder\
    .appName("word_count")\
    .master("local[*]")\
    .getOrCreate()

sc = spark.sparkContext
ssc = StreamingContext(sparkContext=sc, batchDuration=1)

# the topic to subscribe
topic_to_sub = ["test"]
# the address of kafka, separate with comma if there are many
bootstrap_servers = "localhost:9092"
# kafka config info
kafka_params = {"metadata.broker.list": bootstrap_servers}

# initialize stream to consume data from kafka
kafka_stream = KafkaUtils.createDirectStream(ssc=ssc,
                                             topics=topic_to_sub,
                                             kafkaParams=kafka_params)

kafka_stream.pprint()
r = redis.Redis("127.0.0.1")


def save_redis(rdd):
    """
    update word count result into redis hash.

    example:
        word_count_accumulate
            green 9
            blue 3
            red 1

    :param rdd:
    :return:
    """
    # how to use foreachPartition?
    # when partition is used, always _lock is not serializable?
    # does it the problem of redis.py?
    with r.pipeline(transaction=False) as pipe:
        for item in rdd.collect():
            # item is in type of (word, count)
            if not item[0]:
                continue
            update(r, "word_count_accumulate", item[0], item[1])

        pipe.execute()


def update(r_conn, hkey, field, value):
    """
    if field exist already, update it
    else save it
    :param r_conn:
    :param hkey:
    :param field:
    :param value:
    :return:
    """
    # todo: a lua script will help to reduce rtt
    original_value = r_conn.hget(hkey, field)
    if original_value:
        # if exist accumulate
        new_value = value + int(original_value)
    else:
        new_value = value
    r_conn.hset(hkey, field, new_value)


def word_count():
    """
    do word count on streaming data from kafka
    :return:
    """
    # get data
    lines = kafka_stream.map(lambda x: x[1])

    # word count
    counts = lines\
        .flatMap(lambda line: line.split(" "))\
        .map(lambda word: (word, 1))\
        .reduceByKey(lambda x, y: x + y)

    counts.pprint()
    return counts


def main():
    counts = word_count()
    # save to redis
    counts.foreachRDD(save_redis)

    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()






