import sys
sys.path.insert(0, 'commons/')
from pyspark import SparkContext, SparkConf
from Utils import Utils

def splitComma(line):
    splits = Utils.COMMA_DELIMITER.split(line)
    return "{} {}".format(splits[1], splits[2])
if __name__ == "__main__":
    conf = SparkConf().setAppName("airports").setMaster("local[4]")
    sc = SparkContext(conf = conf)
    airports = sc.textFile("airports.txt")
    airportsInUS = airports.filter(lambda line: Utils.COMMA_DELIMITER.split(line)[3] == "\"United States\"")
    airportsNamesAndCityNames = airportsInUS.map(splitComma)
    airportsNamesAndCityNames.saveAsTextFile("out")
