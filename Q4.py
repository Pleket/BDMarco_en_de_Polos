from pyspark import SparkConf, SparkContext, RDD
from pyspark.sql import SparkSession, DataFrame, SQLContext
from pyspark.sql.types import ArrayType, IntegerType, FloatType, DoubleType
from pyspark.sql.functions import split as pyspark_split, col, udf, sum
from pyspark.ml.feature import VectorAssembler

from pyspark.sql.functions import *
from pyspark.sql.types import *
import matplotlib.pyplot as plt
import time
from pyspark import SparkConf, SparkContext
import numpy as np
from numpy import log as ln
from collections import defaultdict
from pyspark.mllib.random import RandomRDDs
import numpy as np
import hashlib

class Q4:
    def __init__(self, epsilon=0.001, delta=0.1):
        # With probability 1-delta, computed answer is within a factor epsilon of the actual answer
        self.epsilon = epsilon
        self.delta = delta

        self.width = math.ceil(math.e / self.epsilon)
        self.depth = math.ceil(ln(1 / self.delta))

    def q4(self, spark_context: SparkContext, rdd: RDD):

        # width = int(np.ceil(np.exp(1) / self.epsilon))
        # depth = int(np.ceil(np.log(1 / self.delta)))

        # define hash functions
        hash = [hashlib.sha1, hashlib.md5, hashlib.sha224, hashlib.sha256]

        # initialize CMS array
        cms = defaultdict(lambda: np.zeros(self.width))

        # function to update CMS with a vector
        def cms_update(vec):
            for i in range(len(vec)):
                for j in range(self.depth):
                    h = hash[j](str(i).encode() + str(vec[i]).encode()).hexdigest()
                    idx = int(h, 16) % self.width
                    cms[j][idx] += 1
            return cms

        # compute CMS sketch of each vector
        cms_vectors = rdd.map(lambda x: (x[0], cms_update(x[1])))
        cms_vectors.foreach(print)