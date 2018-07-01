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
    return str(int(splitVal[0])*60*60 + int(splitVal[1])*60 + int(splitVal[2]))

def trueLookup(arr, err):
    if arr[0]:
        return arr[0]
    else:
        return err

# --------------------- stop data setup ---------------------- #

# Get data for stops
header = stopWords.first()
DataLines = stopWords.filter(lambda lines: lines != header)
DataSplit = DataLines.map(lambda lines: lines.split(','))

# tripID, arrivalID, stopID
stops = DataSplit.map(lambda lines: (lines[0], unix_convert(lines[1]), lines[3]))
# ----------------------------------------------------------- #

# --------------------- trip data setup --------------------- #

header = tripWords.first()
DataLines = tripWords.filter(lambda lines: lines != header)
DataSplit = DataLines.map(lambda lines: lines.split(','))

# tripID routeID
trips = DataSplit.map(lambda lines: (lines[2], line[0]))
# Set up the trips as a key value pair
#tripMap = sc.broadcast(trips.collectAsMap)
# ----------------------------------------------------------- #

stops.saveAsTextFile('/data/output/stop')
tripMap.saveAsTextFile('/data/output/trips')

#features = map(lambda stop, trip: (stop[0], stop[2], trip.myLookup(stop, "No route ID")), stops, tripMap)
#labels = stops.map(lambda stop: stop[1])

#features.saveAsTextFile('/data/output/features')
#labels.saveAsTextFile('/data/output/labels')

