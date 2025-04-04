import pytest
import os
from pyspark import SparkContext, SparkConf

class TestSparkContext():
 
    def setup_method(self):
        self.sc = SparkContext()

    def test_spark_context_params(self):
        SparkContext(master='local[2]')
 
    def test_parallelize(self):
        rdd = self.sc.parallelize( [('a',7), ('a',2), ('b',2)] )
        assert rdd.count() == 3
        assert rdd.collect() == [('a',7), ('a',2), ('b',2)]

    def test_parallelize_iterable(self):
        rdd = self.sc.parallelize( range(5) )
        assert rdd.count() == 5
        assert rdd.collect() == [0, 1, 2, 3, 4]

    def test_textFile(self):
        rdd = self.sc.textFile('example_data/sentences.txt')
        assert rdd.count() == 11
        assert rdd.collect()[4] == 'scrambled it to make a type specimen book. It has'

    def test_wholeTextFiles(self):
        rdd = self.sc.wholeTextFiles('./example_data/')
        assert rdd.count() == 1
        assert rdd.first()[0] == './example_data/sentences.txt'
        
    def test_stop(self):
        assert self.sc.stop() == True

class TestSparkConf():

    def test_function_chaining(self):
        conf = (SparkConf()
            .setMaster("local")
            .setAppName("My app")
            .set("spark.executor.memory", "1g"))
        sc = SparkContext(conf = conf)
        assert sc.master == '//master-url'

class TestRDD():

    def setup_method(self):
        self.rdd = SparkContext().parallelize( [('a',7), ('a',2), ('b',2)] )

    # ---------------
    # Transformations
    # ---------------
    def test_map(self):
        res = self.rdd.map(lambda x: x[0]).collect()
        assert res == ['a', 'a', 'b']

    def test_flatMap(self):
        res = self.rdd.flatMap(lambda x: x).collect()
        assert res == ['a', 7, 'a', 2, 'b', 2]

    def test_flatMapValues(self):
        with pytest.raises(NotImplementedError):
            self.rdd.flatMapValues(lambda x: x)

    def test_mapValues(self):
        res = self.rdd.groupByKey().mapValues(sum).collect()
        assert len(res) == 2
        assert res[0][0] == 'a'
        assert res[0][1] == 9
    
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

    def test_groupBy(self):
        res = self.rdd.groupBy(lambda x: x[1])
        assert res.collect() == [(7, [('a', 7)]), (2, [('a', 2), ('b', 2)])]

    def test_groupByKey(self):
        res = self.rdd.groupByKey()
        assert res.collect() == [('a', [7, 2]), ('b', [2])]
    
    def test_reduceByKey(self):
        res = self.rdd.reduceByKey(lambda x, y: x+y)        
        assert res.collect() == [('a',9), ('b',2)]

    def test_sortBy(self):
        res = self.rdd.sortBy(lambda x: x[1])
        assert res.collect() == [('a',2), ('b',2), ('a',7)]

    def test_sortByKey(self):
        res = self.rdd.sortByKey()
        assert res.collect() == [('a',7), ('a',2), ('b',2)]    

    def test_join_individual_values(self):
        sc = SparkContext()
        leftRDD = sc.parallelize([('a',1), ('b',2), ('c',3)])
        rightRDD = sc.parallelize([('a',10), ('b',20), ('c',30)])

        res = leftRDD.join(rightRDD)
        assert res.collect() == [('a',(1,10)), ('b',(2,20)), ('c',(3, 30))]
    
    def test_join_tuple_values(self):
        sc = SparkContext()
        leftRDD = sc.parallelize([('a',(1,10)), ('b',(2,20)), ('c',(3,30))])
        rightRDD = sc.parallelize([('a',100), ('b',200), ('c',300)])

        res = leftRDD.join(rightRDD)
        assert res.collect() == [('a',(1,10,100)), ('b',(2,20,200)), ('c',(3, 30,300))]

    def test_distinct(self):
        sc = SparkContext()
        rdd = sc.parallelize([('a', 10), ('a', 11), ('a', 10)])
        distinct_rdd = rdd.distinct()
        assert distinct_rdd.count() == 2
 
    def test_aggregate(self):
        with pytest.raises(NotImplementedError):
            self.rdd.aggregate()
    
    def test_aggregateByKey(self):
        with pytest.raises(NotImplementedError):
            self.rdd.aggregateByKey()
    
    def test_fold(self):
        with pytest.raises(NotImplementedError):
            self.rdd.fold()

    def test_foldByKey(self):
        with pytest.raises(NotImplementedError):
            self.rdd.foldByKey()

    def test_keyBy(self):
        with pytest.raises(NotImplementedError):
            self.rdd.keyBy()

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

    def test_histogram(self):
        with pytest.raises(NotImplementedError):
            self.rdd.histogram(3)

    def test_first(self):
        assert self.rdd.first() == ('a', 7)

    def test_take(self):
        assert self.rdd.take(2) == [('a', 7), ('a', 2)]

    def test_top(self):
        assert self.rdd.values().top(1) == [7]

    def test_foreach(self):
        # Very difficult to test foreach because it returns None
        pass

    def test_reduce(self):
        assert self.rdd.values().reduce(lambda x,y: x+y) == 11

    def test_saveAsTextFile(self):
        filename = 'output.txt'
        self.rdd.saveAsTextFile(filename)
        with open(filename, 'r') as f:
            lines = f.read().splitlines()
            assert lines[0] == 'a,7'
        os.remove(filename)

    def test_collectAsMap(self):
        res = self.rdd.collectAsMap()
        assert len(res.keys()) == 2
        assert res['b'] == 2