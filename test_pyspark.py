from pyspark import SparkContext

class TestSparkContext(object):
 
    def setup_method(self):
        self.sc = SparkContext()
 
    def test_parallelize(self):
        rdd = self.sc.parallelize( [('a',7), ('a',2), ('b',2)] )
        assert rdd.count() == 3
        assert rdd.collect() == [('a',7), ('a',2), ('b',2)]

    def test_textFile(self):
        rdd = self.sc.textFile('example_data.txt')
        assert rdd.count() == 11
        assert rdd.collect()[4] == 'scrambled it to make a type specimen book. It has'


class TestRDD(object):

    def setup_method(self):
        self.rdd = SparkContext().parallelize( [('a',7), ('a',2), ('b',2)] )

    # ---------------
    # Transformations
    # ---------------
    def test_map(self):
        res = self.rdd.map(lambda x: x[0]).collect()
        assert res == ['a', 'a', 'b']

    def test_filter(self):
        res = self.rdd.filter(lambda x: x[0] == 'a').collect()
        assert res == [('a',7), ('a',2)]

    # -------
    # Actions
    # -------
    def test_getNumPartitions(self):
        assert self.rdd.getNumPartitions() == 10
    
    def test_collect(self):
        assert self.rdd.collect() == [('a',7),('a',2),('b',2)]

    def test_count(self):
        assert self.rdd.count() == 3

    

    