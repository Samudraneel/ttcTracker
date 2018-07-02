# PySpark implementation will be done here
from pyspark import SparkContext, SparkConf
import common.rddStops as stopRDD
import common.rddTrips as tripRDD
import modelSetup as model

# --------------------- stop data setup ---------------------- #
stops = stopRDD()
rddStops = stops.getStopRDD()
#stops.saveAsTextFile('/data/output/stop')
# ----------------------------------------------------------- #

# --------------------- trip data setup --------------------- #
trips = tripRDD()
tripMap = trips.getTripMap()

#trips.saveAsTextFile('data/output/trips')
# ----------------------------------------------------------- #

features = model.setFeatures(rddStops, tripMap)

labels = model.setLabels(rddStops)

#features.saveAsTextFile('/data/output/features')
#labels.saveAsTextFile('/data/output/labels')
