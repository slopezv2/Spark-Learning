from pyspark import SparkContext, SparkConf
from Utils import Utils



if __name__ == "__main__":
    conf = SparkConf().setAppName('StackOverflow Example').setMaster("local[*]")
    sc = SparkContext(conf = conf)
    total = sc.accumulator(0)
    missingSalaryMidPoint = sc.accumulator(0)
    responseRDD = sc.textFile("2016-stack-overflow-survey-responses.csv")
    processedBytes = sc.accumulator(0)

    def filterResponseFromCanada(response):
        processedBytes.add(len(response.encode('utf-8')))
        splits = Utils.COMMA_DELIMITER.split(response)
        total.add(1)
        if not splits[14]:
            missingSalaryMidPoint.add(1)
        return splits[2] == "Colombia"
    responseFromCanada = responseRDD.filter(filterResponseFromCanada)
    print("Count of responses from Colombia: {}".format(responseFromCanada.count()))
    print("Total count of responses {}".format(total.value))
    print("Count of responses missing salary middle point: {}".format(missingSalaryMidPoint.value))
    print("Number of bytes processed: {}".format(processedBytes))