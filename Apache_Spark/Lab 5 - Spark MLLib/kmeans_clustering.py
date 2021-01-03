"""
A K-means clustering program using MLlib.

This example requires NumPy (http://www.numpy.org/).
"""
from __future__ import print_function

import sys

import numpy as np
from pyspark import SparkContext
from pyspark.mllib.clustering import KMeans


def parseVector(line):
    return np.array([float(x) for x in line.split(' ')])

sc = SparkContext(appName="KMeans")
lines = sc.textFile("kmeans_data.txt")
data = lines.map(parseVector)
k = 2
model = KMeans.train(data, k)
print("*******Final centers: " + str(model.clusterCenters))
print("*******Total Cost: " + str(model.computeCost(data)))
sc.stop()