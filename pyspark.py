import statistics
import random
import functools
from collections import defaultdict

# See PySpark API docs
# https://spark.apache.org/docs/latest/api/python/index.html

class SparkContext(object):
    def __init__(self):
        pass

    def parallelize(self, dataset):
        return RDD(dataset)

    def textFile(self, filename):
        with open(filename, 'r') as f:
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

    def sample(self, with_replacement, proportion, random_seed):
        quantity = int(self.count() * proportion)
        res = random.sample(self.dataset, quantity)
        return RDD(res)

    def groupByKey(self):
        # Group values into lists
        groups = defaultdict(list)
        for k, v in self.dataset:
            groups[k].append(v)

        # Create tuples from dict
        groups_tuples = []
        for k, v in groups.items():
            groups_tuples.append((k, v))

        return RDD(groups_tuples)

    def sortBy(self, f):
        res = sorted(self.dataset, key=f)
        return RDD(res)

    def sortByKey(self):
        return self.sortBy(lambda x: x[0])

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

    def first(self):
        return self.dataset[0]

    def take(self, num):
        return self.dataset[:num]

    def top(self, num):
        s = sorted(self.dataset, reverse=True)
        return s[:num]

    def foreach(self, f):
        for d in self.dataset:
            f(d)

    def reduce(self, f):
        return functools.reduce(f, self.dataset)

    def reduceByKey(self, f):
        return self.groupByKey().map(lambda x: (x[0], f(x[1]))).collect()

    
    def saveAsTextFile(self, filename):
        with open(filename, 'w') as f:
            for obj in self.dataset:
                if isinstance(obj, tuple):
                    line = ','.join([str(x) for x in obj])
                else: 
                    line = str(obj)
                f.write(line + '\n')                
