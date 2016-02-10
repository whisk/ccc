from pyspark import SparkContext, SparkConf
import sys

def extr(line):
  cols = line.split(' ')
  return [cols[5], cols[6]]

sc = SparkContext(appName='Airport Popularity')
data = sc.textFile('hdfs://hadoop-master:9000' + sys.argv[1])

result = data.flatMap(extr).map(lambda airport: (airport, 1)).reduceByKey(lambda a, b: a + b).coalesce(1).sortBy(lambda el: el[1], ascending=False)
result.saveAsTextFile('hdfs://hadoop-master:9000' + sys.argv[2])

sc.stop()
