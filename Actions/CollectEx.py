from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("CollectExample").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    inputWords = ["spark","hadoop", "hive", "spark", "pig", "cassandra", "hadoop"]
    wordRdd = sc.parallelize(inputWords)
    words = wordRdd.collect()
    for word in words:
        print word
