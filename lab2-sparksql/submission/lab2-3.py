# Assignment 3
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

DailyTemperature = schemaTempReadings.where((schemaTempReadings["year"] >= 1950) & (schemaTempReadings["year"] <= 2014)) \
    .groupBy('year', 'month', 'date', 'station') \
    .agg(F.min('value').alias('min'), F.max('value').alias('max'))

avgDailyTemperature = DailyTemperature.withColumn('avgDailyTemperature',
                                                  (DailyTemperature['min'] + DailyTemperature['max']) * 0.5)

avgMonthlyTemperature = avgDailyTemperature.select('year', 'month', 'station', 'avgDailyTemperature') \
    .groupBy('year', 'month', 'station') \
    .agg(F.avg('avgDailyTemperature').alias('avgMonthlyTemperature')) \
    .orderBy('avgMonthlyTemperature', ascending=[False])

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
avgMonthlyTemperature.rdd.saveAsTextFile("BDA/output/A2Q3")

sc.stop()
# ----------------------
