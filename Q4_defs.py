from numpy import log as ln

import math


class Q4:
    def __init__(self, epsilon=0.001, delta=0.1):
        # With probability 1-delta, computed answer is within a factor epsilon of the actual answer
        self.epsilon = epsilon
        self.delta = delta

        self.width = math.ceil(math.e / self.epsilon)
        self.d = math.ceil(ln(1 / self.delta))
    
    