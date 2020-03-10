
class SparkContext(object):
    def __init__(self):
        pass

    def parallelize(self, dataset):
        return RDD(dataset)


class RDD(object):
    def __init__(self, dataset):
        self.dataset = dataset

    def collect(self):
        return self.dataset

