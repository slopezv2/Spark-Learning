import sys
from pyspark import SparkContext, SparkConf
from Utils import Utils

if __name__ == "__main__":
    conf = SparkConf().setAppName("airports not in USA").setMaster("local[*]")
    sc = SparkContext(conf= conf)

    airportsRdd = sc.textFile("../airports/airports.txt")
    countryAndAirporyt = airportsRdd.map(lambda airport:\
        (Utils.COMMA_DELIMITER.split(airport)[3],
        Utils.COMMA_DELIMITER.split(airport)[1]))
    
    airportsByCountry = countryAndAirporyt.groupByKey()
    for coutry, airportName in airportsByCountry.collectAsMap().items():
        print("{} : {}".format(coutry,list(airportName)))