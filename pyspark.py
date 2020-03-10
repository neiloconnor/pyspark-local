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

    def collect(self):
        return self.dataset

