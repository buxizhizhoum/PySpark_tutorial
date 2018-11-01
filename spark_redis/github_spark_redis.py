#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
example of open source project spark-redis
"""
from __future__ import print_function
import os
import json

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType


os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
# pay attention here, jars could be added at here
os.environ['PYSPARK_SUBMIT_ARGS'] = \
    '--jars /home/buxizhizhoum/2-Learning/pyspark_tutorial/jars/spark-redis-2.3.1-SNAPSHOT-jar-with-dependencies.jar ' \
    '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.1 ' \
    'pyspark-shell'


if __name__ == "__main__":
    spark = SparkSession.builder\
        .appName("spark_redis")\
        .master("local[2]")\
        .getOrCreate()

    time_field = StructField("time", IntegerType(), True)
    value_field = StructField("value", DoubleType(), True)
    fields = [time_field, value_field]
    schema = StructType(fields)

    # infer schema
    # load_df = spark\
    #     .read\
    #     .format("org.apache.spark.sql.redis")\
    #     .option("keys.pattern", "meterdata_1000000:1000000_20181101_2215_pttl") \
    #     .option("infer.schema", True)\
    #     .load()
    
    # specify schema
    load_df = spark \
        .read \
        .format("org.apache.spark.sql.redis") \
        .option("keys.pattern", "meterdata_1000000:1000000_20181101_2215_pttl") \
        .schema(schema) \
        .load()

    load_df.printSchema()
    load_df.show()
