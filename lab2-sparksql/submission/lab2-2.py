
# Assignment 2
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row, SparkSession
from pyspark.sql import functions as F

# spark = SparkSession.builder\
#     .appName("Lab")\
#     .getOrCreate()

# temperature_file = spark.sparkContext.textFile("temperature-readings-small.csv")
# lines = temperature_file.map(lambda line: line.split(";"))

sc = SparkContext(appName="Lab")
sqlContext = SQLContext(sc)

temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

tempReadings = lines.map(lambda p: Row(station=p[0],
                                       date=p[1],
                                       year=p[1].split("-")[0],
                                       month=p[1].split("-")[1],
                                       time=p[2],
                                       value=float(p[3]),
                                       quality=p[4]))

schemaTempReadings = sqlContext.createDataFrame(tempReadings)
# schemaTempReadings.registerTempTable("tempReadings")


# part 1
TempCount = schemaTempReadings.where(
    (schemaTempReadings["year"] >= 1950) & (schemaTempReadings["year"] <= 2014) & (schemaTempReadings["value"] >= 10)) \
    .select('year', 'month') \
    .groupBy('year', 'month') \
    .count() \
    .orderBy('count', ascending=[False])

# part 2
TempCountFromStation = schemaTempReadings.where(
    (schemaTempReadings["year"] >= 1950) & (schemaTempReadings["year"] <= 2014) & (schemaTempReadings["value"] >= 10)) \
    .select('year', 'month', 'station') \
    .groupBy('year', 'month', 'station') \
    .count() \
    .groupBy('year', 'month').count() \
    .orderBy('count', ascending=[False])

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
TempCount.rdd.saveAsTextFile("BDA/output/1")
TempCountFromStation.rdd.saveAsTextFile("BDA/output/2")

sc.stop()
# ----------------------