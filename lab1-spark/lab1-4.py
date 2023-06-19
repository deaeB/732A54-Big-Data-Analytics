# Assigment 4
from pyspark import SparkContext

sc = SparkContext(appName = "Lab")

temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
precipitation_file=sc.textFile("BDA/input/precipitation-readings.csv")
temperature_lines = temperature_file.map(lambda line: line.split(";"))
precipitation_lines = precipitation_file.map(lambda line: line.split(";"))

#Temperature
# (key, value) = (station,temperature)
station_temperature = temperature_lines.map(lambda x: (x[0], float(x[3])))\
                    .reduceByKey(lambda x,y:max(x,y))\
                    .filter(lambda x:x[1]>25 and x[1]<30)

#Precipitation
# (key, value) = ((year,month,date,station),precipitation)
station_precipitation= precipitation_lines.map(lambda x: ((x[1][0:4],x[1][5:7],x[1][8:],x[0]), float(x[3])))\
                    .reduceByKey(lambda x,y:x+y)\
                    .map(lambda x:(x[0][3],x[1]))\
                    .reduceByKey(lambda x,y:max(x,y))\
                    .filter(lambda x:x[1]>100 and x[1]<200)

#Join
conbine_data=station_temperature.join(station_precipitation)
'''
Return an RDD containing all pairs of elements with matching keys in self and other.
Each pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in self and (k, v2) is in other.
'''

# print(conbine_data.collect())
# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
conbine_data.saveAsTextFile("BDA/output")

sc.stop()