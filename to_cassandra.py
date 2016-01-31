#!/usr/bin/env python

# 
# Version: 1.0
# Usage: ./to_cassandra.py <Task Name> < data
# Requirements: cassandra-driver

from cassandra.cluster import Cluster
import argparse
import sys
import re
import time

AVAIL_TASKS   = ['Task21', 'Task22', 'Task23', 'Task24', 'Task32']
DEFAULT_HOSTS = ['10.0.223.125', '10.0.55.140']

parser = argparse.ArgumentParser(description='cmdline query tool for CCC Part 1')
parser.add_argument('task', nargs='?',  choices=AVAIL_TASKS, help='Task Name')
parser.add_argument('-k', '--keyspace', default='ccc_1',     help='Cassandra DB Keyspace')
parser.add_argument('--host',           action='append',     help='Cassandra DB Host')
parser.add_argument('-t', '--truncate', action='store_true', help='Truncate before insertion')
args = parser.parse_args()

if args.host == None:
    args.host = DEFAULT_HOSTS

print args

# connect to cassandra
try:
    cluster = Cluster(args.host)
    cass = cluster.connect(args.keyspace)
except Exception as e:
    print e
    sys.exit(1)

def insert_data_simple(table_name, key_name, value_name, value_type=str):
    if args.truncate:
        print "Truncating table ..."
        cass.execute('truncate %s' % table_name);
        print "Done"

    print "Inserting data..."
    cnt = 0
    t = time.time()
    ins_stmt = cass.prepare('insert into %s (%s, %s) values (?, ?)' % (table_name, key_name, value_name))
    for line in sys.stdin:
        line = line.strip()
        (k, v) = re.split('\s+', line, 1)
        v = value_type(v)
        cass.execute(ins_stmt, (k, v))
        cnt += 1
    total_time = time.time() - t
    print "Inserted %d rows" % cnt
    print "Time: %0.4fs (%0.04fs per row)" % (total_time, total_time / cnt)

if args.task == 'Task21':
    insert_data_simple('top_carriers_by_origin', 'origin', 'carriers')
elif args.task == 'Task22':
    insert_data_simple('top_destinations_by_origin', 'origin', 'destinations')
elif args.task == 'Task23':
    insert_data_simple('top_carriers_by_route', 'route', 'carriers')
elif args.task == 'Task24':
    insert_data_simple('arrival_delay_by_route', 'route', 'arrival_delay', float)
    
