# Model training

import time
from pyspark import SparkContext

sc = SparkContext.getOrCreate()
stopTimes = '../ttc/2018-06-29/stop_times.txt'
trip = '../ttc/2018-06-29/trips.txt'

# trip_id, arrival_time, departure_time, stop_id, ......<<no one cares>>............

# Takes in a path to a file, opens it and returns all the contents except for the first row
# Each row is a list all contained within a larger list
def open_txtfile(directory):
    with open(directory) as stopTimes:
        arr = stopTimes.read().split('\n')
        data = list(map(lambda line: line.split(','), arr[1:]))
        data.pop() # There is a [''] element at the end of the list due to '\r' being appended
        stopTimes.close()
    return data

# Converts given clock time into unix time
def unix_convert(val):
        splitVal = val.split(':')
        return str(int(splitVal[0])*60*60 + int(splitVal[1])*60 + int(splitVal[2]))

def convert_time(data):
    data = list(map(lambda line: unix_convert(line[1]), data))

def getOrElse(data, val):
    print val
    if val in data:
        return data[data.index(val)][0]
    else:
        return "No Route ID"

stopData = open_txtfile(stopTimes)
convert_time(stopData)

tripData = open_txtfile(trip)

features = [] # make this a lambda statement
labels = list(map(lambda line: line[1], stopData))

for stop in stopData:
    features.append([stop[0], stop[3], getOrElse(tripData, stop[0])])

print features[0:2], '\n'
print lablels[0:2], '\n'
print 'worked'
