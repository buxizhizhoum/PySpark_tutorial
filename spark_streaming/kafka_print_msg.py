#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
one method to get data from kafka is createStream()
another is createDirectStream()

this is example of createStream() and only print what it consume from kafka
"""
import os

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
os.environ['PYSPARK_SUBMIT_ARGS'] \
    = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.1 ' \
      'pyspark-shell'


sc = SparkContext("local[2]", "Streaming")
sc.setLogLevel("WARN")
ssc = StreamingContext(sc, 2)

zookeeper = "127.0.0.1:2181"
# todo: confirm which is the right one...
kfk_brokers = {"bootstrap_servers": "127.0.0.1:9092",
               "kafka.bootstrap.servers": "127.0.0.1:9092",
               "brokers": "127.0.0.1:9092",
               "host": "127.0.0.1:9092"}
topic = {"test": 1}
group_id = "test_2018"

lines = KafkaUtils.createStream(ssc, zookeeper, group_id, topic,
                                kafkaParams=kfk_brokers)
print(lines)
lines_tmp = lines.map(lambda x: x[1])
lines_tmp.pprint()

ssc.start()
ssc.awaitTermination(10)



