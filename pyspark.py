import statistics
from collections import defaultdict

# See PySpark API docs
# https://spark.apache.org/docs/latest/api/python/index.html

class SparkContext(object):
    def __init__(self):
        pass

    def parallelize(self, dataset):
        return RDD(dataset)

    def textFile(self, filename):
        with open(filename) as f:
            lines = f.read().splitlines()
        return RDD(lines)

    def stop(self):
        return True


class RDD(object):
    def __init__(self, dataset):
        self.dataset = dataset

    # Transformations
    def map(self, f):
        res = list(map(f, self.dataset))
        return RDD(res)

    def filter(self, f):
        res = list(filter(f, self.dataset))
        return RDD(res)

    def keys(self):
        return self.map(lambda x: x[0])

    def values(self):
        return self.map(lambda x: x[1])

    # Actions
    def collect(self):
        return self.dataset

    def getNumPartitions(self):
        return 10

    def count(self):
        return len(self.dataset)

    def countByValue(self):
        counts = defaultdict(int)
        for obj in self.dataset:
            counts[obj] += 1
        return counts

    def countByKey(self):
        return self.keys().countByValue()

    def isEmpty(self):
        return self.dataset == []

    def sum(self):
        return sum(self.dataset)
        
    def max(self):
        return max(self.dataset)
        
    def min(self):
        return min(self.dataset)
        
    def mean(self):
        return statistics.mean(self.dataset)
    
    def stdev(self):
        # stdev function samples the dataset so use pstdev
        return statistics.pstdev(self.dataset)

    def variance(self):
        # variance function samples the dataset so use pvariance
        return statistics.pvariance(self.dataset)