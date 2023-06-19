# Assigment 2-2
from pyspark import SparkContext

sc = SparkContext(appName = "Lab")

temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

# (key, value) = ((year,month,station),temperature)
year_temperature = lines.map(lambda x: ((x[1][0:4],x[1][5:7],x[0]), float(x[3])))\
                    .filter(lambda x: int(x[0][0])>=1950 and int(x[0][0])<=2014 and x[1]>10)

# Reduce the value
year_month_sta_count=year_temperature.reduceByKey(lambda x,y:max(x,y))\
                    .map(lambda x:((x[0][0],x[0][1]),x[1]))\
                    .countByKey()         #Count the number of each key and return a dict

# print(year_month_sta_count.items())
# dict->items->list->RDD->save
sc.parallelize(list(year_month_sta_count.items())).saveAsTextFile("BDA/output")

sc.stop()