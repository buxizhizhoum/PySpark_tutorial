#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
write data to elastic search
https://starsift.com/2018/01/18/integrating-pyspark-and-elasticsearch/
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

es_write_conf = {
    # specify the node that we are sending data to (this should be the master)
    "es.nodes": 'localhost',
    # specify the port in case it is not the default port
    "es.port": '9200',
    # specify a resource in the form 'index/doc-type'
    "es.resource": 'testindex/testdoc',
    # is the input JSON?
    "es.input.json": "yes",
    # is there a field in the mapping that should be used to specify the ES document ID
    "es.mapping.id": "doc_id"
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
    rdd.saveAsNewAPIHadoopFile(
        path='-',
        outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
        keyClass="org.apache.hadoop.io.NullWritable",
        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
        # critically, we must specify our `es_write_conf`
        conf=es_write_conf)





