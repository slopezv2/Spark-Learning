from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("Wordcount with pair RDD reduce").setMaster("local[3]")
    sc = SparkContext(conf = conf)
    lines = sc.textFile("bill.txt")
    wordRdd = lines.flatMap(lambda line: line.split(" "))
    wordPairRdd = wordRdd.map(lambda word: (word,1))
    #Example with reduceByKey takes all the pairs with
    #the same key and applies: val1 + val 2
    wordCounts = wordPairRdd.reduceByKey(lambda x, y: x + y)
    print wordCounts
    temp = wordCounts.collect()
    for word, count in temp:
        print("{} : {}".format(word, count))