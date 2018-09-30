#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
from official spark machine learning example.
examples/src/main/python/ml/correlation_example.py
"""
from __future__ import print_function
import os

from pyspark.ml.linalg import Vectors
from pyspark.ml.stat import ChiSquareTest
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
os.environ['PYSPARK_SUBMIT_ARGS'] = \
    '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 ' \
    '--master local[2] ' \
    'pyspark-shell'


spark = SparkSession\
    .builder\
    .appName("correlation_example")\
    .master("local[2]")\
    .getOrCreate()


def main():
    data = [(0.0, Vectors.dense(0.5, 10.0)),
            (0.0, Vectors.dense(1.5, 20.0)),
            (1.0, Vectors.dense(1.5, 30.0)),
            (0.0, Vectors.dense(3.5, 30.0)),
            (0.0, Vectors.dense(3.5, 40.0)),
            (1.0, Vectors.dense(3.5, 40.0))]

    df = spark.createDataFrame(data, ["label", "features"])
    df.show()

    r = ChiSquareTest.test(df, "features", "label").head()
    print("pValues: ", str(r.pValues))
    print("degreesOfFreedom: ", str(r.degreesOfFreedom))
    print("statistics: ", str(r.statistics))


if __name__ == "__main__":
    main()

