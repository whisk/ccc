from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import sys
import time

BATCH_INTERVAL = 5
hdfs_prefix = 'hdfs://hadoop-master:9000'
idle_time_max = 60.0
TOP_N = 10

top = []
ts_has_data = time.time()
ts_no_data  = time.time()
sc = SparkContext(appName='Airport Popularity')
ssc = StreamingContext(sc, BATCH_INTERVAL)
ssc.checkpoint(hdfs_prefix + '/checkpoint')

def extract_org_dest(line):
  cols = line.split(' ')
  if len(cols) > 6:
    return [cols[5], cols[6]]
  else:
    return []

def top_airports(rdd):
  global top
  global ts_has_data
  global ts_no_data
  global ssc
  curr = rdd.toLocalIterator()
  # concat top and curr values
  top_dict = dict(top)
  total = 0
  for el in curr:
    total += 1
    k = el[0]
    if k in top_dict:
      top_dict[k] += el[1]
    else:
      top_dict[k] = el[1]
  
  top = sorted(top_dict.items(), key=lambda el: el[1], reverse=True)

  if total == 0:
    ts_no_data = time.time()
  else:
    ts_has_data = time.time()
  

  print('=' * 80)
  print(top[:TOP_N])
  print('=' * 80)

dstream = ssc.textFileStream(hdfs_prefix + sys.argv[1])
dstream = dstream.flatMap(extract_org_dest).map(lambda airport: (airport, 1)).reduceByKey(lambda a, b: a + b)
dstream.foreachRDD(top_airports)

ssc.start()
while True:
  res = ssc.awaitTerminationOrTimeout(10)
  if res:
    # stopped
    break
  else:
    # still running
    if ts_no_data - ts_has_data > idle_time_max:
      print("No data received for %s seconds, stopping..." % idle_time_max)
      ssc.stop(stopSparkContext=True, stopGraceFully=True)

