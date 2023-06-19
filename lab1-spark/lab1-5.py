# Assigment 5
from pyspark import SparkContext

sc = SparkContext(appName = "Lab")

precipitation_file=sc.textFile("BDA/input/precipitation-readings.csv")
ost_station_file=sc.textFile("BDA/input/stations-Ostergotland.csv")
precipitation_lines = precipitation_file.map(lambda line: line.split(";"))
ost_station_lines_bc=ost_station_file.map(lambda line: line.split(";")).map(lambda x:int(x[0]))

# broadcast
bc=sc.broadcast(ost_station_lines_bc.collect())

# (key, value) = ((year,month,station),precipitation)
ost_average_monthly_precipitation=precipitation_lines.filter(lambda x:(int(x[0])in bc.value) and \
                                    int(x[1][0:4])>=1993 and int(x[1][0:4])<=2016)\
        .map(lambda x:((x[1][0:4],x[1][5:7],x[0]),float(x[3])))\
        .reduceByKey(lambda x,y:x+y)\
        .map(lambda x:((x[0][0],x[0][1]),x[1]))\
        .groupByKey()\
        .mapValues(lambda x:sum(x)/len(x))

print(ost_average_monthly_precipitation.collect())

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
ost_average_monthly_precipitation.saveAsTextFile("BDA/output")

sc.stop()