# Assigment 2-1
from pyspark import SparkContext

sc = SparkContext(appName = "Lab")

temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

# (key, value) = ((year,month),temperature)
year_temperature = lines.map(lambda x: ((x[1][0:4],x[1][5:7]), float(x[3])))

#filter
year_temperature = year_temperature.filter(lambda x: int(x[0][0])>=1950 and int(x[0][0])<=2014 and x[1]>10)

#(key, value) = ((year,month),count)
year_month_count=year_temperature.map(lambda x:(x[0],1))

#Get counts
year_month_counts = year_month_count.reduceByKey(lambda a,b: a+1)

# print(year_month_counts.collect())

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
year_month_counts.saveAsTextFile("BDA/output")

sc.stop()