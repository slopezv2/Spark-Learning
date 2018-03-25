from pyspark import SparkContext, SparkConf

def isNotHeader(line):
    return not (line.startswith("host") and "bytes" in line)

if __name__ == "__main__":
    # Use only a core
    conf = SparkConf().setAppName("IntersecLogs").setMaster("local[1]")
    sc = SparkContext(conf = conf)
    n1 = sc.textFile("nasa1.tsv")
    n2 = sc.textFile("nasa2.tsv")
    hostsN1 = n1.filter(lambda line: line.split("\t")[0])
    hostsN2 = n2.filter(lambda line: line.split("\t")[0])
    intersection = hostsN1.intersection(hostsN2)
    #not headers
    cleanedLogLines = intersection.filter(lambda host: host != "host")
    cleanedLogLines.saveAsTextFile("outIntersect")