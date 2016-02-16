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
ts_has_data = time.time()
ts_no_data  = time.time()

def get_cass():
  cluster = Cluster(args.cassandra_host)
  return cluster.connect(args.cassandra_keyspace)

def extract_trip_info(line):
  cols = line.split(' ')
  try:
    if cols[0] == "2008":
      date = "%04d-%02d-%02d" % (int(cols[0]), int(cols[1]), int(cols[2]))
      dep_delay = 0
      try:
        dep_delay = round(float(cols[8]))
      except:
        pass
      # origin destination date time dep_delay
      return [(cols[5], cols[6], date, int(cols[7]), dep_delay)]
    else:
      return []
  except Exception as e:
    print(line, e)
    return []

def save_trip_partition(part):
  cass = get_cass()
  prepared_stmt = cass.prepare("insert into trips (origin, destination, departure_date, departure_time, departure_delay) values (?, ?, ?, ?, ?)")
  total = 0
  for el in part:
    total += 1
    try:
      cass.execute(prepared_stmt, (el[0], el[1], el[2], el[3], el[4]))
    except Exception as e:
      print(el, e)
  print("=" * 80)
  print(total)
  cass.shutdown()

def save_trip(rdd):
  global ts_has_data
  global ts_no_data

  if rdd.isEmpty():
    total = 0
  else:
    total = 1
    rdd.foreachPartition(save_trip_partition)

  if total == 0:
    ts_no_data = time.time()
  else:
    ts_has_data = time.time()

get_cass().execute('truncate trips')

sc = SparkContext(appName='Trips')
ssc = StreamingContext(sc, args.batch_interval)
ssc.checkpoint(args.hdfs_prefix + '/checkpoint/q32')

dstream = ssc.textFileStream(args.hdfs_prefix + args.input_dir)
dstream = dstream.flatMap(extract_trip_info).foreachRDD(save_trip)

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

