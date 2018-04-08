import sys
from pyspark import SparkContext, SparkConf
from Utils import Utils

if __name__ == "__main__":
    conf = SparkConf().setAppName("airports not in USA").setMaster("local")
    sc = SparkContext(conf= conf)

    airportsRdd = sc.textFile("airports.txt")
    airportsPairRdd = airportsRdd.map(lambda line: (Utils.COMMA_DELIMITER.split(line)[1],Utils.COMMA_DELIMITER.split(line)[3]))
    upperCase = airportsPairRdd.mapValues(lambda country: country.upper())
    upperCase.saveAsTextFile("outUpper")