# Setting up how the RDD will be handled for stops
# Stops RDD will have to be passed in

from pyspark import SparkContext
import initialize
import helper

class RDDTrips():
    def __init__(self):
        # Initialize all data regarding stops
        sc = SparkContext.getOrCreate()
        data = sc.textFile(initialize._dataDir + initialize._trips)

        # Header required to remove the first row
        header = data.first()

        # Set up the trip RDD
        self.trips = data.filter(lambda lines: lines != header)\
                .map(lambda lines: lines.split(','))\
                .map(lambda lines: (lines[2], lines[0]))

        def getTripRDD():
            return self.trips

        def getTripMap():
            return self.trips.collectAsMap()
