# Assignment 5
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


# ---
precipitation_file = sc.textFile("BDA/input/precipitation-readings.csv")
lines = precipitation_file.map(lambda line: line.split(";"))

precipReadings = lines.map(lambda p: Row(station=p[0],
                      date=p[1],
                      year=p[1].split("-")[0],
                      month=p[1].split("-")[1],
                      time=p[2],
                      value=float(p[3]),
                      quality=p[4]))

schemaPrecipReadings = sqlContext.createDataFrame(precipReadings)

# -----
station_file = sc.textFile("BDA/input/stations-Ostergotland.csv")
lines = station_file.map(lambda line: line.split(";"))

StationReadings = lines.map(lambda p: Row(station=p[0]))

schemaStationReadings = sqlContext.createDataFrame(StationReadings)

# ----
# schemaPrecipReadings.show()
# schemaStationReadings.show()

schemaPrecipReadings = schemaPrecipReadings.join(schemaStationReadings, on = ['station'])

schemaPrecipReadings = schemaPrecipReadings.where((schemaPrecipReadings["year"] >= 1993) & (schemaPrecipReadings["year"] <= 2016))\
    .groupBy("year", 'month', 'station').agg(F.sum('value').alias('totMonPrecip'))\
    .orderBy(["year", 'month'], ascending=[False, False])\
    .groupBy("year", 'month').agg(F.avg('totMonPrecip').alias('avgMonthlyPrecipitation'))


# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
schemaPrecipReadings.rdd.saveAsTextFile("BDA/output")


sc.stop()
# ----------------------

