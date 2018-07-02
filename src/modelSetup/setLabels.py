# Initialize labels

def setLabels(stopsRDD):
    labels = stopsRDD.map(lambda stop: stop[1])\
            .filter(lambda line: line != None)
    return labels
