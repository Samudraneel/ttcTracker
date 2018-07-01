# PySpark implementation will be done here
from pyspark import SparkContext, SparkConf

# Initialize SparkContext
conf = SparkConf().setAppName("Test App")
sc = SparkContext(conf=conf)

# Read data
_dir = '/data/ttc/2018-06-29/'

stopWords = sc.textFile(_dir + 'stop_times.txt')
tripWords = sc.textFile(_dir + 'trips.txt')

def unix_convert(val):
    splitVal = val.split(':')
    return str(int(splitVal[0])*60*60 + int(splitVal[1])*60 + int(splitVal))

def trueLookup(arr, err):
    if arr[0]:
        return arr[0]
    else:
        return err

def f(x):
    return x.split(',')

# tripID, arrivalID, stopID
stops = map(lambda line: (line[0], unix_convert(line[1]), line[3]), stopWords.foreach(f))

# tripID, routeID
trips = map(lambda line: (line[2], line[0]), tripArr)

# Set up the trips as a key value pair
tripMap = sc.broadcast(trips.collectAsMap)

features = map(lambda stop, trip: (stop[0], stop[2], trip.myLookup(stop, "No route ID")), stops, tripMap)
labels = map(lambda stop: stop[1], stops)

features.saveAsTextFile('/data/output/features')
labels.saveAsTextFile('/data/output/labels')

#stopWords.saveAsTextFile('/data/output/stop_times')
#tripWords.saveAsTextFile('/data/output/trips')
