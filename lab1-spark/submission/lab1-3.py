#According to the requirements at the beginning of the file, the algorithm of average temperature needs to be changed.
# Assigment 3
from pyspark import SparkContext

sc = SparkContext(appName = "Lab")

temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

# (key, value) = ((year,month,date,station),temperature)
year_temperature = lines.map(lambda x: ((x[1][0:4],x[1][5:7],x[1][8:],x[0]), float(x[3])))\
                    .filter(lambda x: int(x[0][0])>=1960 and int(x[0][0])<=2014)

# print(year_temperature.collect())

# Compute the month average temperature for each station
#daily max temperature
max_sta_temperature=year_temperature.reduceByKey(lambda x,y:max(x,y))
#daily min temperature
min_sta_temperature=year_temperature.reduceByKey(lambda x,y:min(x,y))
#Average monthly temperature
average_sta_temperature=max_sta_temperature.join(min_sta_temperature)\
                        .map(lambda x:((x[0][0],x[0][1],x[0][3]),(x[1][0]+x[1][1])/2))\
                        .groupByKey()\
                        .mapValues(lambda x:sum(x)/len(x))
'''
groupByKey: Group the values for each key in the RDD into a single sequence. Hash-partitions the resulting RDD with numPartitions partitions.
mapValues: Pass each value in the key-value pair RDD through a map function without changing the keys; this also retains the original RDDs partitioning.
'''

print(average_sta_temperature.collect())

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
average_sta_temperature.saveAsTextFile("BDA/output")

sc.stop()
