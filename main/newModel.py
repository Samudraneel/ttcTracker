from pyspark import SparkContext

sc = SparkContext.getOrCreate()
stopTimes = '../ttc/2018-06-29/stop_times.txt'
trip = '../ttc/2018-06-29/trips.txt'

stopData = sc.textFile(stopTimes).collect()

print stopData[0:10]

