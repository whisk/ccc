#!/usr/bin/env python

# 
# Version: 1.0
# Usage: ./query.py <Task Name>
# Requirements: cassandra-driver

from cassandra.cluster import Cluster
import argparse
import sys
import re

AVAIL_TASKS   = ['Task21', 'Task22', 'Task23', 'Task24', 'Task32']
DEFAULT_HOSTS = ['10.0.223.125', '10.0.55.140']

parser = argparse.ArgumentParser(description='cmdline query tool for CCC Part 1')
parser.add_argument('task', nargs='?',  choices=AVAIL_TASKS, help='Task Name')
parser.add_argument('-k', '--keyspace', default='ccc_1',     help='Cassandra DB Keyspace')
parser.add_argument('--host',           action='append',     help='Cassandra DB Host')
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

if args.task == 'Task21':
    print "Enter origin code: "
    origin = raw_input().strip().upper()
    rows = []
    for row in cass.execute('select origin, carriers from top_carriers_by_origin where origin = %s', [origin]):
        rows += [row]
    if len(rows) == 0:
        print "Nothing found for %s" % origin
    else:
        print "Top Carriers for %s:" % origin
        for z in rows[0].carriers.split(' '):
            [c, r] = z.split('=')
            print "%s %0.2f" % (c, float(r))

