from pyspark.sql import SparkSession

AGE_MIDPOINT = "age_midpoint"
SALARY_MIDPOINT = "salary_midpoint"
SALARY_MIDPOINT_BUCKET = "salary_midpoint_bucket"

if __name__ == "__main__":
    session = SparkSession.builder.appName("StackOverflow sql").master("local[1]").getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    dataFrameReader = session.read 

    responses = dataFrameReader \
        .option("header","true") \
        .option("inferSchema", value = True) \
        .csv("2016-stack-overflow-survey-responses.csv")

    print("Schema:")
    responses.printSchema()

    responseWithSelectedColums =responses.select("country","occupation",AGE_MIDPOINT, SALARY_MIDPOINT)
    print("Print selected columns")
    responseWithSelectedColums.show()

    print("Print records where the response is from Colombia")
    responseWithSelectedColums \
        .filter(responseWithSelectedColums["country"]=="Colombia").show()
    print("Print the count of occupations")
    groupedData = responseWithSelectedColums.groupBy("occupation")
    groupedData.count().show()

    print(" print records with average mid age less than 20")
    responseWithSelectedColums \
        .filter(responseWithSelectedColums[AGE_MIDPOINT] < 20 ).show()
    
    print(" Result by salary middle point in descending order")
    responseWithSelectedColums \
    .orderBy(responseWithSelectedColums[SALARY_MIDPOINT], ascending = False).show()

    print("Group by country and aggregate by avg salary middle point")
    dataGroupByCountry = responseWithSelectedColums.groupBy("country")
    dataGroupByCountry.avg(SALARY_MIDPOINT).show()

    responseWithSalaryBucket = responses.withColumn(SALARY_MIDPOINT_BUCKET,((responses[SALARY_MIDPOINT]/20000).cast("integer")*20000))
    print("with salary bucket column")
    responseWithSalaryBucket.select(SALARY_MIDPOINT, SALARY_MIDPOINT_BUCKET).show()
    print("Group by salary bucket")
    responseWithSalaryBucket \
        .groupBy(SALARY_MIDPOINT_BUCKET) \
        .count() \
        .orderBy(SALARY_MIDPOINT_BUCKET) \
        .show()
    session.stop()

