# Any tests will be done here

from pyspark import SparkContext

sc = SparkContext.getOrCreate()
data = sc.textFile('/data/ttc/2018-06-29/stop_times.txt')
lines = data.map(lambda line: line.split(','))
line = lines.map(lambda val: (val[0], val[1]))

lines.saveAsTextFile('/data/output/xd')
