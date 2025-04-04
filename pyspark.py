import os
import statistics
import random
import functools
from collections import defaultdict
from collections.abc import Iterable

# See PySpark API docs
# https://spark.apache.org/docs/latest/api/python/index.html

class SparkContext():
    def __init__(self, *args, **kwargs):
        self.version = 'pyspark-local'
        self.pythonVer = '3.x'
        self.master = '//master-url'
        self.sparkHome = '/usr/bin/sparkHome'
        self.sparkUser = lambda : 'root'
        self.appName = 'default'
        self.applicationId = '1'
        self.defaultParallelism = 2
        self.defaultMinPartitions = 2
    
    def parallelize(self, dataset):
        if isinstance(dataset, Iterable):
            dataset = list(dataset)
        return RDD(dataset)

    def textFile(self, filename):
        with open(filename, 'r', encoding='UTF-8') as f:
            lines = f.read().splitlines()
        return RDD(lines)
    
    def wholeTextFiles(self, directory_name):
        file_names = os.listdir(directory_name)
        whole_files = []
        for file_name in file_names:
            path = directory_name + file_name
            with open(path, 'r', encoding='UTF-8') as f:
                whole_files.append((path, f.read()))
        return RDD(whole_files)

    def stop(self):
        return True

class SparkConf():
    def __init__(self):
        pass

    def setMaster(self, master_url):
        return self # Return self for method chaining
    
    def setAppName(self, name):
        return self # Return self for method chaining
    
    def set(self, key, value):
        return self # Return self for method chaining

class RDD():
    def __init__(self, dataset):
        self.dataset = dataset

    # Transformations
    def map(self, f):
        res = list(map(f, self.dataset))
        return RDD(res)

    def flatMap(self, f):
        list_of_lists_rdd = self.map(f)
        res = []
        for l in list_of_lists_rdd.dataset:
            for v in l:
                res.append(v)
        return RDD(res)
    
    def flatMapValues(self, f):        
        raise NotImplementedError('flatMapValues is not currently supported by pyspark-local') 

    def mapValues(self, f):
        # Pass each value in the key-value pair RDD through a map function without changing the keys
        return self.map(lambda x: (x[0], f(x[1])))

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

    def groupBy(self, f):
        # Group records into lists keyed by the result of function f
        groups = defaultdict(list)
        for obj in self.dataset:
            groups[f(obj)].append(obj)

        # Create tuples from dict
        groups_tuples = []
        for k, v in groups.items():
            groups_tuples.append((k, v))

        return RDD(groups_tuples)

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
    
    def reduceByKey(self, f):
        # First group by key
        groups = self.groupByKey()

        # For each key reduce all the values using the function provided
        groups_reduction = []
        for (k, v) in groups.collect():
            reduction = functools.reduce(f, v)
            groups_reduction.append((k, reduction))

        # Convert the list of tuples into a new RDD before returning it
        return RDD(groups_reduction)

    def sortBy(self, f):
        res = sorted(self.dataset, key=f)
        return RDD(res)

    def sortByKey(self):
        return self.sortBy(lambda x: x[0])
    
    def join(self, other):
        # Return an RDD containing all pairs of elements with matching keys in self and other
        joined = []
        for k, v in self.dataset:
            # Check for a match in other
            for other_k, other_v in other.dataset:
                if k == other_k:
                    # Ensure that both values are tuples so they can be joined
                    if not isinstance(v, Iterable):
                        v_tuple = (v, )
                    else:
                        v_tuple = v

                    if not isinstance(other_v, Iterable):
                        other_v_tuple = (other_v, )
                    else:
                        other_v_tuple = other_v

                    # Both values are now tuples, so join them
                    joined.append( (k, v_tuple + other_v_tuple) )
                    break
        return RDD(joined)

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
    
    def histogram(self, bins):
        raise NotImplementedError('histogram is not currently supported by pyspark-local')

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
    
    def saveAsTextFile(self, filename):
        with open(filename, 'w') as f:
            for obj in self.dataset:
                if isinstance(obj, tuple):
                    line = ','.join([str(x) for x in obj])
                else: 
                    line = str(obj)
                f.write(line + '\n')

    def collectAsMap(self):
        # Return key,value pairs as a dictionary
        res = {}
        for (k, v) in self.dataset:
            res[k] = v
        return res             


if __name__ == "__main__":
    # If run as a script to emulate pyspark shell
    sc = SparkContext()
    