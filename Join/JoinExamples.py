from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("Join Examples").setMaster("local[1]")
    sc = SparkContext(conf = conf)

    ages = sc.parallelize([("Tom", 29),("John", 22)])
    addreses = sc.parallelize([("James", "USA"),("John", "UK")])

    join = ages.join(addreses)
    join.saveAsTextFile("join")

    leftOuterJoin = ages.leftOuterJoin(addreses)
    leftOuterJoin.saveAsTextFile("leftOuterJoin")

    rightOuterJoin = ages.rightOuterJoin(addreses)
    rightOuterJoin.saveAsTextFile("rightOuterJoin")

    fullOuterJoin = ages.fullOuterJoin(addreses)
    fullOuterJoin.saveAsTextFile("fullOuterJoin")