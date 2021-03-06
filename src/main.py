# PySpark implementation will be done here
from pyspark import SparkContext
import common.rddStops as stopRDD
import common.rddTrips as tripRDD
import modelSetup as model

sc = SparkContext.getOrCreate()
sc.addPyFile('/data/ttcTracker/src/all.zip')

# --------------------- stop data setup ---------------------- #
stops = stopRDD.RDDStops()
stops.rdd.saveAsTextFile('/data/output/stop')
# ----------------------------------------------------------- #

# --------------------- trip data setup --------------------- #
trips = tripRDD.RDDTrips()

trips.rdd.saveAsTextFile('data/output/trips')
# ----------------------------------------------------------- #

features = model.features.setFeatures(stops.rdd, trips.map)

labels = model.labels.setLabels(stops.rdd)

features.saveAsTextFile('/data/output/features')
labels.saveAsTextFile('/data/output/labels')
