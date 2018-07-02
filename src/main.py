# PySpark implementation will be done here
from pyspark import SparkContext, SparkConf
import common.rddStops as stopRDD
import common.rddTrips as tripRDD
import modelSetup as model

# --------------------- stop data setup ---------------------- #
stops = stopRDD.RDDStops()
rddStops = stops.getStopRDD()
#stops.saveAsTextFile('/data/output/stop')
# ----------------------------------------------------------- #

# --------------------- trip data setup --------------------- #
trips = tripRDD.RDDTrips()
tripMap = trips.getTripMap()

#trips.saveAsTextFile('data/output/trips')
# ----------------------------------------------------------- #

features = model.features.setFeatures(rddStops, tripMap)

labels = model.labels.setLabels(rddStops)

#features.saveAsTextFile('/data/output/features')
#labels.saveAsTextFile('/data/output/labels')
