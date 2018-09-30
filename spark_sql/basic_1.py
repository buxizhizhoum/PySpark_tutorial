#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
example from spark official document
"""
import os

from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import StructType

os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
os.environ['PYSPARK_SUBMIT_ARGS'] = \
    '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 ' \
    '--master local[2] ' \
    'pyspark-shell'

conf = SparkConf().setAppName("basic_exercise")
sc = SparkContext(conf=conf)
spark = SparkSession\
    .builder\
    .config(conf=conf)\
    .getOrCreate()

# lines = sc.textFile("examples/src/main/resources/people.txt")
lines = sc.textFile("/usr/local/spark/examples/src/main/resources/people.txt")
parts = lines.map(lambda line: line.split(", "))
# define column name
people = parts.map(lambda line: (line[0], line[1].strip()))
print(people.take(10))

schema_cols = "name age"
fields = [StructField(field_name, StringType(), True) for field_name in schema_cols.split()]

schema = StructType(fields)

schema_people = spark.createDataFrame(people, schema)
schema_people.createOrReplaceTempView("people")

teenagers = spark.sql("select * from people where age >= 13 and age <= 28")
teenagers.show()

rdd = teenagers.rdd.map(lambda x: (x.name, x.age))
print(rdd.collect())


