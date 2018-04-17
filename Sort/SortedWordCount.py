from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("SortBy example").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    lines = sc.textFile("../wordcount/bill.txt")
    wordRDD = lines.flatMap(lambda line: line.split(" "))
    wordPairRDD = wordRDD.map(lambda word: (word,1))
    wordToCountPairs = wordPairRDD.reduceByKey(lambda x, y: x + y)
    #Sorting
    sortedWordCount = wordToCountPairs.sortBy(lambda wordCount: wordCount[1], ascending = False)
    for word, count in sortedWordCount.collect():
        print("{} : {}".format(word, count))