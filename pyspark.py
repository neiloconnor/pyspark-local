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

    # Actions
    def collect(self):
        print(self.dataset)
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
        keys_rdd = self.map(lambda x: x[0])
        return keys_rdd.countByValue()


