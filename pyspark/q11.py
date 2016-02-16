from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from cassandra.cluster import Cluster
import argparse
import sys
import time

parser = argparse.ArgumentParser()
parser.add_argument('input_dir',                         default='/ccc/input')
parser.add_argument('-n',                                default=10)
parser.add_argument('-p', '--hdfs-prefix',               default='hdfs://hadoop-master:9000')
parser.add_argument('-b', '--batch-interval',            default=5)
parser.add_argument('-k', '--cassandra-keyspace',        default='ccc_1')
parser.add_argument('-t', '--cassandra-truncate',        default=True, action='store_true')
parser.add_argument('--cassandra-host', action='append', default=['cassandra-1', 'cassandra-2'])
parser.add_argument('--idle-time',                       default=30)
parser.add_argument('--run-interval',                    default=5)
args = parser.parse_args()

# 
cassandra_table_name = 'airport_popularity'
top = []
ts_has_data = time.time()
ts_no_data  = time.time()

def get_cass():
  cluster = Cluster(args.cassandra_host)
  return cluster.connect(args.cassandra_keyspace)

def extract_org_dest(line):
  cols = line.split(' ')
  if len(cols) > 6:
    return [(cols[5] 1), (cols[6] 1)]
  else:
    return []

def top_airports(rdd):
  global top
  global ts_has_data
  global ts_no_data
  global args

  cass = get_cass()

  # iterate locally on driver (master) host
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
  print(top[:args.n])
  print('=' * 80)
  prepared_stmt = cass.prepare('insert into %s (airport, popularity) values (?, ?)' % cassandra_table_name)
  for el in top:
    cass.execute(prepared_stmt, (el[0], el[1]))

get_cass().execute('truncate %s' % cassandra_table_name)

sc = SparkContext(appName='Airport Popularity')
ssc = StreamingContext(sc, args.batch_interval)
ssc.checkpoint(args.hdfs_prefix + '/checkpoint')

dstream = ssc.textFileStream(args.hdfs_prefix + args.input_dir)
dstream = dstream.flatMap(extract_org_dest).reduceByKey(lambda a, b: a + b).foreachRDD(top_airports)

ssc.start()
while True:
  res = ssc.awaitTerminationOrTimeout(args.run_interval)
  if res:
    # stopped elsewhere
    break
  else:
    # still running
    if ts_no_data - ts_has_data > args.idle_time:
      print("No data received for %s seconds, stopping..." % args.idle_time)
      ssc.stop(stopSparkContext=True, stopGraceFully=True)

