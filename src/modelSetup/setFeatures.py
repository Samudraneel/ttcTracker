# Initialize features

import src.commons.rddStops as rddStops
import src.commons.rddTrips as rddTrips

stopsRDD = rddStops.getStopRDD()
tripsMap = rddTrips.getTripMap()

features = stopsRDD.map(lambda stop: (stop[0], stop[2], tripsMap.get(stop[0], 'No route ID found')))

def getFeatures():
    return features
