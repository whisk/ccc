from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import sys

BATCH_INTERVAL = 10
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
  curr_top = rdd.top(TOP_N, key=lambda el: el[1])
  # summ top and curr_top values
  top_dict = {}
  for el in top + curr_top:
    k = el[0]
    if k in top_dict:
      top_dict[k] += el[1]
    else:
      top_dict[k] = el[1]

  top = sorted(top_dict.items(), key=lambda el: el[1], reverse=True)[:TOP_N]

  print('=' * 80)
  print(top)
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
