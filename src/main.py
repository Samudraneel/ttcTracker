# PySpark implementation will be done here
from pyspark import SparkContext, SparkConf
import common.rddStops as stopRDD
import common.rddTrips as tripRDD
import modelSetup as model

# --------------------- stop data setup ---------------------- #
stops = stopRDD.RDDStops()
stops.rdd.saveAsTextFile('/data/output/stop')
# ----------------------------------------------------------- #

# --------------------- trip data setup --------------------- #
trips = tripRDD.RDDTrips()

trips.map.saveAsTextFile('data/output/trips')
# ----------------------------------------------------------- #

features = model.features.setFeatures(stops.rdd, tripMap)

labels = model.labels.setLabels(stops.rdd)

features.saveAsTextFile('/data/output/features')
labels.saveAsTextFile('/data/output/labels')
