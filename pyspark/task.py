from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from cassandra.cluster import Cluster
import argparse
import sys
import time
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition

AVAIL_TASKS=['q11', 'q12', 'q13', 'q21', 'q22', 'q23', 'q24', 'q32']

parser = argparse.ArgumentParser()
parser.add_argument('task', choices=AVAIL_TASKS)
parser.add_argument('--hdfs-input-dir',                  default='/ccc/input')
parser.add_argument('--kafka-topic',                     default='ccc_1')
parser.add_argument('--zookeeper',                       default='kafka-1:2181')
parser.add_argument('--kafka-partitions', type=int,      default=1)
parser.add_argument('-n', type=int,                      default=10)
parser.add_argument('-p', '--hdfs-prefix',               default='hdfs://hadoop-master:9000')
parser.add_argument('-b', '--batch-interval',            default=5)
parser.add_argument('-k', '--cassandra-keyspace',        default='ccc_1')
parser.add_argument('-t', '--cassandra-truncate',        default=False, action='store_true')
parser.add_argument('--cassandra-host', action='append', default=['cassandra-1', 'cassandra-2'])
parser.add_argument('--idle-time', type=int,             default=60)
parser.add_argument('--run-interval', type=int,          default=5)
args = parser.parse_args()

# 
_schema = {
  'q11': {'table': 'airport_popularity',         'key': 'airport', 'value': 'popularity'},
  'q12': {'table': 'carrier_performance',        'key': 'carrier', 'value': 'arrival_delay'},
  'q13': {'table': 'weekday_performance',        'key': 'weekday', 'value': 'arrival_delay'},
  'q21': {'table': 'top_carriers_by_origin',     'key': 'origin',  'value': 'carriers'},
  'q22': {'table': 'top_destinations_by_origin', 'key': 'origin',  'value': 'destinations'},
  'q23': {'table': 'top_carriers_by_route',      'key': 'route',  'value': 'carriers'},
  'q24': {'table': 'arrival_delay_by_route',     'key': 'route',  'value': 'arrival_delay'},
  # q32 is uniq
}
schema = _schema[args.task]

# utility functions
def get_cass():
  cluster = Cluster(args.cassandra_host)
  return cluster.connect(args.cassandra_keyspace)

def dump(*params):
  print('=' * 20)
  print(time.strftime("%x %X"))
  print('=' * 20)
  print(params)

def extr_line(line):
  return line[1].split(' ')

# extractors
# q11
def extract_origin_destination(line):
  cols = extr_line(line)
  if len(cols) > 6:
    return [(cols[5], 1), (cols[6], 1)]
  else:
    return []

# q12
def extract_carr_arr_delay(line):
  cols = extr_line(line)
  try:
    return [(cols[4], (float(cols[9]), 1))]
  except:
    return []

# q13
def extract_weekday_arr_delay(line):
  cols = extr_line(line)
  try:
    return [(int(cols[3]), (float(cols[9]), 1))]
  except:
    return []

# q21
def extract_origin_carrier_dep_delay(line):
  cols = extr_line(line)
  try:
    # "origin-carrier" (delay 1)
    return [(cols[5] + "-" + cols[4], (float(cols[8]), 1))]
  except:
    return []

# q22
def extract_origin_destination_dep_delay(line):
  cols = extr_line(line)
  try:
    # "origin-destination" (delay 1)
    return [(cols[5] + "-" + cols[6], (float(cols[8]), 1))]
  except:
    return []

# q23
def extract_route_carrier_arr_delay(line):
  cols = extr_line(line)
  try:
    # "origin-destination" (delay 1)
    return [(cols[5] + "_" + cols[6] + "-" + cols[4], (float(cols[9]), 1))]
  except:
    return []

# q24
def extract_route_arr_delay(line):
  cols = extr_line(line)
  try:
    # "origin-destination" (delay 1)
    return [(cols[5] + "_" + cols[6], (float(cols[9]), 1))]
  except:
    return []

# q32
def extract_trip_info(line):
  cols = extr_line(line)
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

# rdds
# rdds global vars
top = []
ts_last_data = None

# rdds functions
# top by count, q11
def top_count(rdd):
  global top
  global ts_last_data

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
  
  top = top_dict.items()
  dump(len(top))

  if total > 0:
    ts_last_data = time.time() 

  prepared_stmt = cass.prepare('insert into %s (%s, %s) values (?, ?)' % (schema['table'], schema['key'], schema['value']))
  for el in top:
    cass.execute(prepared_stmt, (el[0], el[1]))

  cass.shutdown()

# top by average, q12, q13
def top_average(rdd):
  global top
  global ts_last_data

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
      top_dict[k] = (top_dict[k][0] + el[1][0], top_dict[k][1] + el[1][1])
    else:
      top_dict[k] = el[1]
  
  top = top_dict.items()
  dump(len(top))

  if total > 0:
    ts_last_data = time.time() 

  prepared_stmt = cass.prepare('insert into %s (%s, %s) values (?, ?)' % (schema['table'], schema['key'], schema['value']))
  for el in top:
    cass.execute(prepared_stmt, (el[0], el[1][0] / el[1][1]))

  cass.shutdown()

# top by average on a complex key
def top_complex_average(rdd):
  global top
  global ts_last_data

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
  dump(len(top))

  if total > 0:
    ts_last_data = time.time() 

  prepared_stmt = cass.prepare('insert into %s (%s, %s) values (?, ?)' % (schema['table'], schema['key'], schema['value']))
  for origin in top:
    carriers = ' '.join(["%s=%0.2f" % (el[0], el[1][0] / el[1][1]) for el in sorted(top[origin].items(), key=lambda el: el[1][0] / el[1][1])][:args.n])
    cass.execute(prepared_stmt, (origin, carriers))

  cass.shutdown()

# q32 
# partition saving to cassandra
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
  dump(total)
  cass.shutdown()

# rdd save
def save_trip(rdd):
  global ts_last_data

  if not rdd.isEmpty():
    ts_last_data = time.time() 
    rdd.foreachPartition(save_trip_partition)

# truncation
if args.t:
  get_cass().execute('truncate %s' % schema['table'])

# main part
sc = SparkContext(appName='Task %s' % args.task)
ssc = StreamingContext(sc, args.batch_interval)
ssc.checkpoint(args.hdfs_prefix + '/checkpoint/' + args.task)

dstream = KafkaUtils.createStream(ssc, \
  args.zookeeper, 'task-%s-consumer' % args.task, {args.kafka_topic : args.kafka_partitions}) 

# task specific
if args.task == 'q11':
  dstream = dstream.flatMap(extract_origin_destination).reduceByKey(lambda a, b: a + b).foreachRDD(top_count)
elif args.task == 'q12':
  dstream = dstream.flatMap(extract_carr_arr_delay).reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).foreachRDD(top_average)
elif args.task == 'q13':
  dstream = dstream.flatMap(extract_weekday_arr_delay).reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).foreachRDD(top_average)
elif args.task == 'q21':
  dstream = dstream.flatMap(extract_origin_carrier_dep_delay).reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).foreachRDD(top_complex_average)
elif args.task == 'q22':
  dstream = dstream.flatMap(extract_origin_destination_dep_delay).reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).foreachRDD(top_complex_average)
elif args.task == 'q23':
  dstream = dstream.flatMap(extract_route_carrier_arr_delay).reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).foreachRDD(top_complex_average)
elif args.task == 'q24':
  dstream = dstream.flatMap(extract_route_arr_delay).reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).foreachRDD(top_average)
elif args.task == 'q32':
  dstream = dstream.flatMap(extract_trip_info).foreachRDD(save_trip)
else:
  print("Unknown task")

# runner
ts_last_data = time.time()
ssc.start()
while True:
  res = ssc.awaitTerminationOrTimeout(args.run_interval)
  if res:
    # stopped elsewhere
    break
  else:
    # still running
    if time.time() - ts_last_data > args.idle_time:
      print("No data received for %d seconds, stopping..." % args.idle_time)
      ssc.stop(stopSparkContext=True, stopGraceFully=True)