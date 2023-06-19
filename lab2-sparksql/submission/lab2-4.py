# Assignment 4
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
precipitation_file = sc.textFile("BDA/input/precipitation-readings.csv")
# ---
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

# -----
lines = precipitation_file.map(lambda line: line.split(";"))

precipReadings = lines.map(lambda p: Row(station=p[0],
                                         date=p[1],
                                         year=p[1].split("-")[0],
                                         month=p[1].split("-")[1],
                                         time=p[2],
                                         value=float(p[3]),
                                         quality=p[4]))

schemaPrecipReadings = sqlContext.createDataFrame(precipReadings)

# ----
# schemaTempReadings.show()
# schemaPrecipReadings.show()

MaxTemp = schemaTempReadings.groupBy('station')\
    .agg(F.max('value').alias('maxTemp'))

MaxTemp = MaxTemp.where((MaxTemp['maxTemp'] >=25) & (MaxTemp['maxTemp'] <=30))

MaxPrecip = schemaPrecipReadings.groupBy('station', 'date')\
    .agg(F.sum('value').alias('totDailyPrecipitation'))\
    .groupBy('station').agg(F.max('totDailyPrecipitation').alias('maxDailyPrecipitation'))

MaxPrecip = MaxPrecip.where( (MaxPrecip['maxDailyPrecipitation'] >= 100) & (MaxPrecip['maxDailyPrecipitation'] <= 200))

stationList = MaxTemp.join(MaxPrecip, on = ['station'])\
    .select('station', 'maxTemp', 'maxDailyPrecipitation')\
    .orderBy('station', ascending=[False])

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
stationList.rdd.saveAsTextFile("BDA/output")

sc.stop()
# ----------------------
