import sys
sys.path.insert(0, 'commons/')
from pyspark import SparkContext, SparkConf
from Utils import Utils

def splitComma(line):
    splits = Utils.COMMA_DELIMITER.split(line)
    return "{} {}".format(splits[1], splits[6])
if __name__ == "__main__":
    conf = SparkConf().setAppName("airports").setMaster("local[1]")
    sc = SparkContext(conf = conf)
    airports = sc.textFile("airports.txt")
    # Airports in latitude > 40
    airportsInUS = airports.filter(lambda line: float(Utils.COMMA_DELIMITER.split(line)[6]) > 40) 
    airportsNamesAndCityNames = airportsInUS.map(splitComma)
    airportsNamesAndCityNames.saveAsTextFile("outLatitude")