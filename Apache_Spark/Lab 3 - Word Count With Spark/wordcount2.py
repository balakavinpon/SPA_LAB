from pyspark import SparkConf, SparkContext
import collections

print("Packages imported!")

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

print('Able to contact Spark!')

# split the words from each line of file
words = sc.textFile("input.txt").flatMap(lambda line: line.split(" "))

# count the occurrence of each word
wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)

# save the counts to output
wordCounts.saveAsTextFile("output")
