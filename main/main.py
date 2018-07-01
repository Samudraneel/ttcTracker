# PySpark implementation will be done here
from pyspark import SparkContext, SparkConf

# Initialize SparkContext
conf = SparkConf().setAppName("Test App")
sc = SparkContext(conf=conf)

# Read data
_dir = '~/myProjs/data/ttc/2018-06-29/'

stopWords = sc.textFile(_dir + 'stop_times.txt')
tripWords = sc.textFile(_dir + 'trips.txt')

stopWords.saveAsTextFile('/data/output/stop_times')
tripWords.saveAsTextFile('/data/output/trips')
