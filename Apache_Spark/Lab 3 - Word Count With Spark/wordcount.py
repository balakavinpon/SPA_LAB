from pyspark import SparkConf, SparkContext
import collections

print("Packages imported!")

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

print('Able to contact Spark!')


lines = sc.textFile("input.txt")
words = lines.map(lambda x: x.split()[0])
result = words.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
print("*****************")
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
print("*****************")

#words = sc.textFile("input.txt").flatMap(lambda line: line.split(" "))
# count the occurrence of each word
#wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)
# save the counts to output
#wordCounts.saveAsTextFile("output")
