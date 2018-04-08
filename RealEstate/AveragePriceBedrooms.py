from pyspark import SparkContext, SparkConf
from AvgCount import AvgCount

if __name__ == "__main__":
    conf = SparkConf().setAppName("avgHousePrice").setMaster("local")
    sc = SparkContext(conf = conf)
    lines  = sc.textFile("RealEstate.csv")
    cleanedLines = lines.filter(lambda line: "Bedrooms" not in line)
    housePricePairRdd = cleanedLines.map(lambda line:\
    (line.split(",")[3], AvgCount(1,float(line.split(",")[2]))))
    housePriceTotal = housePricePairRdd \
        .reduceByKey(lambda x , y: AvgCount(x.count + y.count,x.total + y.total ))
    print("housePriceTotal: \n")
    for bedroom, avgCount in housePriceTotal.collect():
        print("{}: ({}, {})".format(bedroom, avgCount.count, avgCount.total))
    housePriceAvg = housePriceTotal.mapValues(lambda avgCount: avgCount.total / avgCount.count)
    print("House price avg: \n")
    for bedroom, avg in housePriceAvg.collect():
        print("{} : {}".format(bedroom,avg))