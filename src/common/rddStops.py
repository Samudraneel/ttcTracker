# Setting up how the RDD will be handled for stops
# Stops RDD will have to be passed in

from pyspark import SparkContext
import config

class RDDStops():
    def __init__(self):
        # Initialize all data regarding stops
        sc = SparkContext.getOrCreate()
        data = sc.textFile(config._dataDir + config._stops)

        # Header required to remove the first row
        header = data.first()
        self.stops = data.filter(lambda lines: lines != header)\
                    .map(lambda lines: lines.split(','))\
                    .map(lambda lines: (lines[0], self.unix_convert(lines[1]), lines[3]))

    def getStopRDD(self):
        return self.stops

    def unix_convert(self, val):
        if val == 'arrival_time':
            return
        splitVal = val.split(':')
        return int(splitVal[0])*60*60 + int(splitVal[1])*60 + int(splitVal[2])
