#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
write data to elastic search
https://starsift.com/2018/01/18/integrating-pyspark-and-elasticsearch/

this is based on rdd, and df could also be write to and read from elasticsearch
for df read and write refer to:
https://stackoverflow.com/a/52199097/8025086
"""
from __future__ import print_function
import os
import json

from pyspark import SparkContext
from pyspark import SparkConf


os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
# pay attention here, jars could be added at here
os.environ['PYSPARK_SUBMIT_ARGS'] = \
    '--jars /home/buxizhizhoum/2-Learning/pyspark_tutorial/jars/elasticsearch-hadoop-6.4.2/dist/elasticsearch-spark-20_2.11-6.4.2.jar ' \
    '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.1 ' \
    'pyspark-shell'

conf = SparkConf().setAppName("write_es").setMaster("local[2]")
sc = SparkContext(conf=conf)

# refer: https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html
es_read_conf = {
    # specify the node that we are sending data to (this should be the master)
    "es.nodes": 'localhost',
    # specify the port in case it is not the default port
    "es.port": '9200',
    # specify a resource in the form 'index/doc-type'
    "es.resource": 'testindex/testdoc'
}


if __name__ == "__main__":
    data = [
        {'1': '2', 'doc_id': 1},
        {'2': '4', 'doc_id': 2},
        {'3': '8', 'doc_id': 3},
        {'4': '16', 'doc_id': 4},
        {'5': '32', 'doc_id': 5},
        {'6': '64', 'doc_id': 6},
        {'7': '128', 'doc_id': 7},
        {'8': '256', 'doc_id': 8},
        {'9': '512', 'doc_id': 9},
        {'10': '1024', 'doc_id': 10}
    ]
    rdd = sc.parallelize(data)
    rdd = rdd.map(lambda x: (x["doc_id"], json.dumps(x)))

    es_rdd = sc.newAPIHadoopRDD(
        inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
        keyClass="org.apache.hadoop.io.NullWritable",
        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
        conf=es_read_conf)

    print(es_rdd.take(10))





