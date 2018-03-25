from pyspark import SparkContext, SparkConf

def isNotHeader(line):
    return not (line.startswith("host") and "bytes" in line)

if __name__ == "__main__":
    # Use all the local cores
    conf = SparkConf().setAppName("unionLogs").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    n1 = sc.textFile("nasa1.tsv")
    n2 = sc.textFile("nasa2.tsv")
    union3 = n1.union(n2)
    cleanedLogLines = union3.filter(isNotHeader)
    sampleUnion = cleanedLogLines.sample(withReplacement = True, fraction = 0.1)
    sampleUnion.saveAsTextFile("outUnion")