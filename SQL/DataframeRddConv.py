import sys
from pyspark.sql import SparkSession
from Utils import Utils

def mapResponseRdd(line):
    splits = Utils.COMMA_DELIMITER.split(line)
    float1 = None if not splits[6] else float(splits[6])
    float2 = None if not splits[14] else float(splits[14])
    return splits[2], float1, splits[9], float2 

def getColNames(line):
    splits = Utils.COMMA_DELIMITER.split(line)
    return [splits[2], splits[6], splits[9], splits[14]]

if  __name__ == "__main__":
    session = SparkSession.builder.appName("Datafrea and RDD conversions").master("local[*]").getOrCreate()
    sc = session.sparkContext

    lines = sc.textFile("2016-stack-overflow-survey-responses.csv")

    responseRdd = lines \
        .filter(lambda line: not Utils.COMMA_DELIMITER.split(line)[2] == "country") \
        .map(mapResponseRdd)
    
    colNames = lines \
        .filter(lambda line: Utils.COMMA_DELIMITER.split(line)[2] == "country") \
        .map(getColNames)
    
    responseDataFrame = responseRdd.toDF(colNames.collect()[0])
    print("Schema:")
    responseDataFrame.printSchema()

    print("Data sample:")
    responseDataFrame.show(20)

    for response in responseDataFrame.rdd.take(10):
        print(response)