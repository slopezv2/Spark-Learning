from pyspark import SparkContext, SparkConf, StorageLevel

if __name__ == "__main__":
    conf = SparkConf().setAppName("PersistExample").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    sc.setLogLevel("ERROR")
    inputIntegers = [1,2,3,4,5,6]
    integerRdd = sc.parallelize(inputIntegers)
    integerRdd.persist(StorageLevel.MEMORY_ONLY)
    res1 = integerRdd.reduce(lambda x, y:x*y)
    res2 = integerRdd.count()
    print("Res Reduce: {}, Res Count: {}".format(res1, res2))