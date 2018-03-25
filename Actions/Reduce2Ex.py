from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("Reduce2Example").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    sc.setLogLevel("ERROR")
    lines = sc.textFile("primeNums.txt")
    numbers = lines.flatMap(lambda line: line.split("\t"))
    #Clean input strings
    validNumbers = numbers.filter(lambda number: number)
    intNumber = validNumbers.map(lambda number : int(number))
    print("Sum: {}".format(intNumber.reduce(lambda x,y: x+y)))