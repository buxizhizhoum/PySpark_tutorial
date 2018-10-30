#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
from: https://github.com/RedisLabs/spark-redis/blob/master/doc/python.md
do not need to add
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
    '--jars /home/buxizhizhoum/2-Learning/pyspark_tutorial/jars/spark_redis/spark-redis-0.3.2.jar' \
    'pyspark-shell'

spark = SparkSession\
    .builder\
    .appName("word_count")\
    .master("local[*]")\
    .getOrCreate()

sc = spark.sparkContext
ssc = StreamingContext(sparkContext=sc, batchDuration=1)

load_df = spark\
    .read\
    .format("org.apache.spark.sql.redis")\
    .option("table", "person")\
    .load()

load_df.printSchema()
load_df.show()

