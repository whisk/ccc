#!/usr/bin/env python

# 
# Version: 1.0
# Usage: ./query.py <Task Name>
# Requirements: cassandra-driver

from cassandra.cluster import Cluster
import argparse
import sys
import re
import datetime

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

def simple_query(table_name, key_name, key):
    rows = []
    for row in cass.execute('select * from %s where %s = %%s' % (table_name, key_name), [key]):
        rows += [row]
    return rows

def display_list(rows, val, label):
    if len(rows) == 0:
        print 'Nothing found'
    else:
        print label
        for z in getattr(rows[0], val).split(' '):
            [c, r] = z.split('=')
            print "%s %0.2f" % (c, float(r))

def display_val(rows, val, fmt, label):
    if len(rows) == 0:
        print 'Nothing found'
    else:
        print label
        print fmt % getattr(rows[0], val)

if args.task == 'Task21':
    print "Enter origin code: "
    origin = raw_input().strip().upper()
    rows = simple_query('top_carriers_by_origin', 'origin', origin)
    display_list(rows, 'carriers', 'Top Carriers for %s' % origin)
elif args.task == 'Task22':
    print "Enter origin code: "
    origin = raw_input().strip().upper()
    rows = simple_query('top_destinations_by_origin', 'origin', origin)
    display_list(rows, 'destinations', 'Top Destinations for %s' % origin)
elif args.task == 'Task23':
    print "Enter origin code: "
    origin = raw_input().strip().upper()
    print "Enter destination code: "
    dest = raw_input().strip().upper()
    rows = simple_query('top_carriers_by_route', 'route', origin + '_' + dest)
    display_list(rows, 'carriers', 'Top Destinations for route %s -> %s' % (origin, dest))
elif args.task == 'Task24':
    print "Enter origin code: "
    origin = raw_input().strip().upper()
    print "Enter destination code: "
    dest = raw_input().strip().upper()
    rows = simple_query('arrival_delay_by_route', 'route', origin + '_' + dest)
    display_val(rows, 'arrival_delay', '%0.2f', 'Mean Arrival Delay for route %s -> %s' % (origin, dest))
elif args.task == 'Task32':
    print "Enter X Y Z YYYY-MM-DD: "
    (x, y, z, dep_date_raw) = re.split('\s+', raw_input().strip().upper())
    (y, m, d) = dep_date_raw.split('-')
    dd1 = datetime.date(int(y), int(m), int(d));
    dd2 = datetime.date(int(y), int(m), int(d) + 2);
    leg1 = []
    print dd1.strftime('%Y-%m-%d')
    for row in cass.execute('select * from trips where origin = %s and destination = %s and departure_date = %s', [x, y, dd1.strftime('%Y-%m-%d')]):
        leg1 += [row]
    print leg1
    leg2 = []
    for row in cass.execute('select * from trips where origin = %s and destination = %s and departure_date = %s', [y, z, dd2.strftime('%Y-%m-%d')]):
        leg2 += [row]
    print leg2

