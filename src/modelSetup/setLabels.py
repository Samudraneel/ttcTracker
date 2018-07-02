# Initialize labels

import commons.rddStops as rddStops

stopsRDD = rddStops.getStopRDD()

labels = stops.map(lambda stop: stop[1])

def getLabels():
    return labels
