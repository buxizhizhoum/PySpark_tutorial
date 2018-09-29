#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
word count with kafka
consume data from kafka topic "test" and do word count with batch duration of 1 second
"""
from __future__ import print_function
import os

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


def main():
    # initialize stream to consume data from kafka
    kafka_stream = KafkaUtils\
        .createDirectStream(ssc=ssc,
                            topics=topic_to_sub,
                            kafkaParams=kafka_params)

    kafka_stream.pprint()
    # get data
    lines = kafka_stream.map(lambda x: x[1])

    # word count
    counts = lines\
        .flatMap(lambda line: line.split(" "))\
        .map(lambda word: (word, 1))\
        .reduceByKey(lambda x, y: x + y)

    counts.pprint()

    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()
