from pyspark import SparkContext, SparkConf
from AvgCount import AvgCount

if __name__ == "__main__":
    conf = SparkConf().setAppName("avgHousePrice").setMaster("local")
    sc = SparkContext(conf = conf)
    lines  = sc.textFile("../RealEstate/RealEstate.csv")
    cleanedLines = lines.filter(lambda line: "Bedrooms" not in line)
    housePricePairRdd = cleanedLines.map(lambda line:\
    (line.split(",")[3], AvgCount(1,float(line.split(",")[2]))))
    housePriceTotal = housePricePairRdd \
        .reduceByKey(lambda x , y: AvgCount(x.count + y.count,x.total + y.total ))
    housePriceAvg = housePriceTotal.mapValues(lambda avgCount: avgCount.total / avgCount.count)
    sortedHousePriceAvg = housePriceAvg.sortByKey(ascending = False)
    for bedroom, avg in sortedHousePriceAvg.collect():
        print("{} : {}".format(bedroom,avg))