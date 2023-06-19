# Assignment 1
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row, SparkSession
from pyspark.sql import functions as F

sc = SparkContext(appName="Lab")
sqlContext = SQLContext(sc)

temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

# (key, value) = (year,temperature)
tempReadings = lines.map(lambda p: Row(station=p[0],
                      date=p[1],
                      year=p[1].split("-")[0],
                      time=p[2],
                      value=float(p[3]),
                      quality=p[4]))

schemaTempReadings = sqlContext.createDataFrame(tempReadings)
schemaTempReadings.registerTempTable("tempReadings")


TempMinByYearStation = schemaTempReadings.where((schemaTempReadings["year"] >= 1950) & (schemaTempReadings["year"] <= 2014)). \
    groupBy('year', 'station'). \
    agg(F.min('value').alias('annualMin')). \
    select('year', 'station', 'annualMin')
    # orderBy(['annualMin'], ascending=[False])

TempMinByYear = schemaTempReadings.groupBy('year'). \
    agg(F.min('value').alias('annualMin'))

TempMinByYearWithStation = TempMinByYearStation.join(TempMinByYear, on=['year', 'annualMin'])\
    .select('year', 'station', 'annualMin')\
    .orderBy(['annualMin'], ascending=[False])

# ------max
TempMaxByYearStation = schemaTempReadings.where((schemaTempReadings["year"] >= 1950) & (schemaTempReadings["year"] <= 2014)). \
    groupBy('year', 'station'). \
    agg(F.max('value').alias('annualMax')). \
    select('year', 'station', 'annualMax')
    # orderBy(['annualMax'], ascending=[False])

TempMaxByYear = schemaTempReadings.groupBy('year'). \
    agg(F.max('value').alias('annualMax'))

TempMaxByYearWithStation = TempMaxByYearStation.join(TempMaxByYear, on=['year', 'annualMax'])\
    .select('year', 'station', 'annualMax')\
    .orderBy(['annualMax'], ascending=[False])




# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
TempMaxByYearWithStation.rdd.saveAsTextFile("BDA/output/max")
TempMinByYearWithStation.rdd.saveAsTextFile("BDA/output/min")


sc.stop()
# ----------------------
