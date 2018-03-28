from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("PairRDDFromRegularRdd").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    inputStrings = ["Lilly 23","Juan 56", "John 56", "Jacinto 56", "Marianne 42"]
    regularRDD = sc.parallelize(inputStrings)
    pairRDD = regularRDD.map(lambda s: (s.split(" ")[0], s.split(" ")[1]))
    #Reduce to 1 partition
    pairRDD.coalesce(1).saveAsTextFile("out2")