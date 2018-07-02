# Setting up how the RDD will be handled for stops
# Stops RDD will have to be passed in

from pyspark import SparkContext
import initialize
import helper

# Initialize all data regarding stops
sc = SparkContext.getOrCreate()
data = sc.textFile(initialize._dataDir + initialize._stops)

# Header required to remove the first row
header = data.first()
stops = data.filter(lambda lines: lines != header)\
        .map(lambda lines: lines.split(','))\
        .map(lambda lines: (lines[0], local.unix_convert(lines[1]), lines[3]))

def getStopRDD():
    return stops
