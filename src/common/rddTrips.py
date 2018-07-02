# Setting up how the RDD will be handled for stops
# Stops RDD will have to be passed in

from pyspark import SparkContext
import config

class RDDTrips():
    def __init__(self):
        # Initialize all data regarding stops
        sc = SparkContext.getOrCreate()
        self.data = sc.textFile(config._dataDir + config._trips)

        # Header required to remove the first row
        header = self.data.first()

        # Set up the trip RDD
        self.rdd = self.data.filter(lambda lines: lines != header)\
                    .map(lambda lines: lines.split(','))\
                    .map(lambda lines: (lines[2], lines[0]))

        self.map = self.rdd.collectAsMap()
