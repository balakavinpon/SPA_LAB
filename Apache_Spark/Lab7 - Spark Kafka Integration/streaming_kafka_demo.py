# -*- coding: utf-8 -*-
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
ssc = StreamingContext(sc, 2)

kvs = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {'t1':1})

lines = kvs.map(lambda x: x[1])

counts = lines.flatMap(lambda line: line.split(" ")) \
                  .map(lambda word: (word, 1)) \
                  .reduceByKey(lambda a, b: a+b)
counts.pprint()
ssc.start()
ssc.awaitTermination()