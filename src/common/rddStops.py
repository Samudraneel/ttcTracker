# Setting up how the RDD will be handled for stops
# Stops RDD will have to be passed in

from pyspark import SparkContext
import config

class RDDStops():
    def __init__(self):
        # Initialize all data regarding stops
        sc = SparkContext.getOrCreate()
        self.data = sc.textFile(config._dataDir + config._stops)
        
        header = self.data.first()
        self.rdd = self.data.filter(lambda lines: lines != header)\
                    .map(lambda lines: lines.split(','))\
                    .map(lambda lines: (lines[0], unix_convert(lines[1]), lines[3]))

# TODO: Move this to a helper function
def unix_convert(val):
    if val == 'arrival_time':
        return
    splitVal = val.split(':')
    return int(splitVal[0])*60*60 + int(splitVal[1])*60 + int(splitVal[2])
