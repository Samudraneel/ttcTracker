# PySpark implementation will be done here
from pyspark import SparkContext, SparkConf

# Initialize SparkContext
conf = SparkConf().setAppName("Test App")
sc = SparkContext(conf=conf)

# Read data
_dir = '/data/ttc/2018-06-29/'

stopData = sc.textFile(_dir + 'stop_times.txt')
tripData = sc.textFile(_dir + 'trips.txt')

def unix_convert(val):
    if val == 'arrival_time':
        return
    splitVal = val.split(':')
    return int(splitVal[0])*60*60 + int(splitVal[1])*60 + int(splitVal[2])

def trueLookup(arr, err):
    if arr[0]:
        return arr[0]
    else:
        return err

# --------------------- stop data setup ---------------------- #

header = stopData.first()
stops = stopData.filter(lambda lines: lines != header)\
        .map(lambda lines: lines.split(','))\
        .map(lambda lines: (lines[0], unix_convert(lines[1]), lines[3]))

#stops.saveAsTextFile('/data/output/stop')
# ----------------------------------------------------------- #

# --------------------- trip data setup --------------------- #

header = tripData.first()
trips = tripData.filter(lambda lines: lines != header)\
        .map(lambda lines: lines.split(','))\
        .map(lambda lines: (lines[2], lines[0]))

#trips.saveAsTextFile('data/output/trips')
# ----------------------------------------------------------- #


#features = map(lambda stop, trip: (stop[0], stop[2], trip.myLookup(stop, "No route ID")), stops, tripMap)
labels = stops.map(lambda stop: stop[1] != None)

#features.saveAsTextFile('/data/output/features')
labels.saveAsTextFile('/data/output/labels')

