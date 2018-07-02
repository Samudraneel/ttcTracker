# Initialize features

print 'Imported setFeatures'

def setFeatures(stopsRDD, tripsMap):
    features = stopsRDD.map(lambda stop: (stop[0], stop[2], tripsMap.get(stop[0], 'No route ID found')))
    return features
