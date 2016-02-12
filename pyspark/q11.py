from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import sys

BATCH_INTERVAL = 5
hdfs_prefix = 'hdfs://hadoop-master:9000'
TOP_N = 10

top = []

def extract_org_dest(line):
  cols = line.split(' ')
  if len(cols) > 6:
    return [cols[5], cols[6]]
  else:
    return []

def top_airports(rdd):
  global top
  curr = rdd.toLocalIterator()
  # concat top and curr values
  top_dict = dict(top)
  for el in curr:
    k = el[0]
    if k in top_dict:
      top_dict[k] += el[1]
    else:
      top_dict[k] = el[1]

  top = sorted(top_dict.items(), key=lambda el: el[1], reverse=True)

  print('=' * 80)
  print(top[:TOP_N])
  print('=' * 80)

sc = SparkContext(appName='Airport Popularity')
ssc = StreamingContext(sc, BATCH_INTERVAL)
ssc.checkpoint(hdfs_prefix + '/checkpoint')
dstream = ssc.textFileStream(hdfs_prefix + sys.argv[1])
dstream = dstream.flatMap(extract_org_dest).map(lambda airport: (airport, 1)).reduceByKey(lambda a, b: a + b)
dstream.pprint()
dstream.foreachRDD(top_airports)

ssc.start()
ssc.awaitTermination()
