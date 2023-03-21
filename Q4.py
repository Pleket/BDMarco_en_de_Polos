from pyspark import SparkContext, RDD
from pyspark.sql.functions import split as sum

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkContext
import numpy as np
from numpy import log as ln
from collections import defaultdict
import numpy as np
import random as rd

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
        hash = []
        for i in range(self.depth):
            prime = rd.choice([x for x in range(self.width, self.width * 2) if not [t for t in range(2, x) if not x % t]])
            hash.append(Hash_Function(prime, self.width))

        # initialize CMS array
        cms = defaultdict(lambda: np.zeros(self.width))

        # function to update CMS with a vector
        def cms_update(vec):
            for i in range(len(vec)):
                for j in range(self.depth):
                    idx = hash[j].get_index(str(i).encode() + str(vec[i]).encode())
                    cms[j][idx] += 1
            return cms

        # compute CMS sketch of each vector
        cms_vectors = rdd.map(lambda x: (x[0], cms_update(x[1])))
        cms_vectors.foreach(print)

class Hash_Function:
    def __init__(self, mod_prime, mod_width):
        self.mod_prime = mod_prime
        self.mod_width = mod_width
        self.multiple = rd.randint(1, 30)
    
    def get_index(self, key):
        return ((key * self.multiple) % self.mod_prime) % self.mod_width