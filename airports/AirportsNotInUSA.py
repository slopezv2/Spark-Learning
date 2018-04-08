import sys
from pyspark import SparkContext, SparkConf
from Utils import Utils

if __name__ == "__main__":
    conf = SparkConf().setAppName("airports not in USA").setMaster("local")
    sc = SparkContext(conf= conf)

    airportsRdd = sc.textFile("airports.txt")
    airportsPairRdd = airportsRdd.map(lambda line: (Utils.COMMA_DELIMITER.split(line)[1],Utils.COMMA_DELIMITER.split(line)[3]))
    airportsNotInUSA = airportsPairRdd.filter(lambda keyValue: keyValue[1] != "\"United States\"")
    airportsNotInUSA.saveAsTextFile("outNotInUsa") 