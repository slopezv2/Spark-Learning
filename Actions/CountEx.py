from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("CountExample").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    inputWords = ["spark","hadoop", "hive", "spark", "pig", "cassandra", "hadoop"]
    wordRdd = sc.parallelize(inputWords)
    print("Count: {}".format(wordRdd.count()))
    wordCountByValue = wordRdd.countByValue()
    print("CountByValue: ")
    for word, count in wordCountByValue.items():
        print("{}, {}".format(word, count))