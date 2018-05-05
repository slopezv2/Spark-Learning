from pyspark.sql import SparkSession, functions as fs

if __name__ == "__main__":
    session = SparkSession.builder.appName("SQL Join").master("local[*]").getOrCreate()
    makerSpace = session.read.option("header", "true") \
        .csv("uk-makerspaces-identifiable-data.csv")

    postCode = session.read.option("header", "true") \
        .csv("uk-postcodes.csv").withColumn("PostCode", fs.concat_ws("", fs.col("PostCode"),fs.lit(" ")))

    print("Maker data example")
    makerSpace.select("Name of makerspace","Postcode").show()

    print("Postcodes data example")
    postCode.select("PostCode", "Region").show()

    joined = makerSpace \
        .join(postCode,makerSpace["Postcode"].startswith(postCode["Postcode"]), "left_outer")

    print (" Group By Region:")
    joined.groupBy("Region").count().show(200)