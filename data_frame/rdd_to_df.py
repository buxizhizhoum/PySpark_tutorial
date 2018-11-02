#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
rdd to df in spark 1.x
https://stackoverflow.com/a/32788832/8025086
"""

from __future__ import print_function
import os

from pyspark import SparkContext
from pyspark.sql import SparkSession


os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
# pay attention here, jars could be added at here
os.environ['PYSPARK_SUBMIT_ARGS'] = 'pyspark-shell'

# SQLContext or HiveContext in Spark 1.x
sc = SparkContext()

rdd = sc.parallelize([("a", 1)])
print(hasattr(rdd, "toDF"))
# False

spark = SparkSession(sc)
print(hasattr(rdd, "toDF"))
# True

rdd.toDF().show()
