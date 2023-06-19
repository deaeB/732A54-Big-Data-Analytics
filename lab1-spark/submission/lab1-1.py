# Assignment 1
from pyspark import SparkContext

sc = SparkContext(appName = "Lab")

temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

# (key, value) = (year,temperature)
year_temperature = lines.map(lambda x: (x[1][0:4], float(x[3])))

#filter
year_temperature = year_temperature.filter(lambda x: int(x[0])>=1950 and int(x[0])<=2014)

#Get max
max_temperatures = year_temperature.reduceByKey(lambda a,b: max(a, b))
max_temperatures = max_temperatures.sortBy(ascending = False, keyfunc=lambda k: k[1])

#Get min
min_temperatures = year_temperature.reduceByKey(lambda a,b: min(a, b))
min_temperatures = min_temperatures.sortBy(ascending = False, keyfunc=lambda k: k[1])

# print(max_temperatures.collect())
# print("____________________________________________")
# print(min_temperatures.collect())

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
max_temperatures.saveAsTextFile("BDA/output/max")
min_temperatures.saveAsTextFile("BDA/output/min")

sc.stop()
