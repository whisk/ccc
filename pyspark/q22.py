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
cassandra_table_name       = 'top_destinations_by_origin'
cassandra_table_key_name   = 'origin'
cassandra_table_value_name = 'destinations'
top = []
ts_has_data = time.time()
ts_no_data  = time.time()

def get_cass():
  cluster = Cluster(args.cassandra_host)
  return cluster.connect(args.cassandra_keyspace)

def extract_origin_destination_dep_delay(line):
  cols = line.split(' ')
  try:
    # "origin-destination" (delay 1)
    return [(cols[5] + "-" + cols[6], (float(cols[8]), 1))]
  except:
    return []

def top_weekdays(rdd):
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
    key    = el[0].split('-')[0]
    subkey = el[0].split('-')[1]
    if key in top_dict:
      if subkey in top_dict[key]:
        top_dict[key][subkey] = (top_dict[key][subkey][0] + el[1][0], top_dict[key][subkey][1] + el[1][1])
      else:
        top_dict[key][subkey] = el[1]
    else:      
      top_dict[key] = {subkey: el[1]}

  top = top_dict

  if total == 0:
    ts_no_data = time.time()
  else:
    ts_has_data = time.time()

  print('=' * 80)
  print(top.keys())
  print('=' * 80)
  prepared_stmt = cass.prepare('insert into %s (%s, %s) values (?, ?)' % (cassandra_table_name, cassandra_table_key_name, cassandra_table_value_name))
  for origin in top:
    carriers = ' '.join(["%s=%0.2f" % (el[0], el[1][0] / el[1][1]) for el in sorted(top[origin].items(), key=lambda el: el[1][0] / el[1][1])][:args.n])
    cass.execute(prepared_stmt, (origin, carriers))

get_cass().execute('truncate %s' % cassandra_table_name)

sc = SparkContext(appName='Top Destinations By Origin')
ssc = StreamingContext(sc, args.batch_interval)
ssc.checkpoint(args.hdfs_prefix + '/checkpoint/q22')

dstream = ssc.textFileStream(args.hdfs_prefix + args.input_dir)
dstream = dstream.flatMap(extract_origin_destination_dep_delay).reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).foreachRDD(top_weekdays)

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

