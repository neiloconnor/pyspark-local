import pytest
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

    def test_stop(self):
        assert self.sc.stop() == True

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

    def test_keys(self):
        res = self.rdd.keys().collect()
        assert res == ['a', 'a', 'b']

    def test_values(self):
        res = self.rdd.values().collect()
        assert res == [7, 2, 2]

    def test_sample(self):
        res = self.rdd.sample(False, 0.5, 7)
        assert res.count() == 1

        res = self.rdd.sample(False, 0.7, 7)
        assert res.count() == 2

    # -------
    # Actions
    # -------
    def test_getNumPartitions(self):
        assert self.rdd.getNumPartitions() == 10
    
    def test_collect(self):
        assert self.rdd.collect() == [('a',7),('a',2),('b',2)]

    def test_count(self):
        assert self.rdd.count() == 3

    def test_countByValue(self):
        assert self.rdd.countByValue() == {('a',7):1,('a',2):1,('b',2):1}

    def test_countByKey(self):
        assert self.rdd.countByKey() == {'a':2, 'b':1}

    def test_isEmpty(self):
        assert self.rdd.isEmpty() == False

    def test_sum(self):
        assert self.rdd.values().sum() == 11

    def test_max(self):
        assert self.rdd.values().max() == 7

    def test_min(self):
        assert self.rdd.values().min() == 2

    def test_mean(self):
        assert self.rdd.values().mean() == pytest.approx(3.66, 0.01)

    def test_stdev(self):
        assert self.rdd.values().stdev() == pytest.approx(2.35, 0.01)

    def test_variance(self):
        assert self.rdd.values().variance() == pytest.approx(5.55, 0.01)

    def test_first(self):
        assert self.rdd.first() == ('a', 7)

    def test_take(self):
        assert self.rdd.take(2) == [('a', 7), ('a', 2)]

    def test_top(self):
        assert self.rdd.values().top(1) == [7]

    def test_foreach(self):
        # Very difficult to test foreach because it returns None
        pass