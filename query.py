#!/usr/bin/env python3

# 
# Version: 1.0
# Usage: ./query.py <Task Name>
# Requirements: cassandra-driver

from cassandra.cluster import Cluster
import argparse
import sys
import re
import datetime

AVAIL_TASKS=['q11', 'q12', 'q13', 'q21', 'q22', 'q23', 'q24', 'q32']

parser = argparse.ArgumentParser(description='cmdline query tool for CCC')
parser.add_argument('task', nargs='?',  choices=AVAIL_TASKS,                                         help='Task Name')
parser.add_argument('-n',               type=int,            default=10)
parser.add_argument('-k', '--keyspace',                      default='ccc_1',                        help='Cassandra DB Keyspace')
parser.add_argument('--host',           action='append',     default=['cassandra-1', 'cassandra-2'], help='Cassandra DB Host')
args = parser.parse_args()

# connect to cassandra
try:
    cluster = Cluster(args.host)
    cass = cluster.connect(args.keyspace)
except Exception as e:
    print(e)
    sys.exit(1)

def simple_query(table_name, key_name, key):
    rows = []
    for row in cass.execute('select * from %s where %s = %%s' % (table_name, key_name), [key]):
        rows += [row]
    return rows

def display_list(rows, val, label):
    if len(rows) == 0:
        print('Nothing found')
    else:
        print(label)
        for z in getattr(rows[0], val).split(' '):
            [c, r] = z.split('=')
            print("%s %0.2f" % (re.sub(r'~.*', '', c), float(r)))

def flight_query(x, y, d, time_min, time_max):
    flights = []
    for row in cass.execute('select * from trips where origin = %s and destination = %s and departure_date = %s', [x, y, d.strftime('%Y-%m-%d')]):
        if row.departure_time >= time_min and row.departure_time <= time_max:
            flights += [row]

    flights = sorted(flights, key=lambda f: f.departure_delay)
    return flights[0] if len(flights) > 0 else None

def display_flight(f):
    if f is None:
        print("Not found")
    else:
        print("%s->%s date %s time %04s, Delay %d" % (f.origin, f.destination, f.departure_date, f.departure_time, f.departure_delay))

def display_val(rows, val, fmt, label):
    if len(rows) == 0:
        print('Nothing found')
    else:
        print(label)
        print(fmt % getattr(rows[0], val))

def display_top(table_name, key_name, value_name, sort_func, fmt, key_type, value_type):
    rows = []
    for row in cass.execute('select * from %s' % (table_name)):
        rows += [row]    
    for el in sorted(rows, key=sort_func)[:args.n]:
        print(fmt % (key_type(getattr(el, key_name)), value_type(getattr(el, value_name))))

if args.task == 'q11':
    print("Top airports")
    display_top('airport_popularity', 'airport', 'popularity', lambda x: -x.popularity, "%4s: %12s", str, int)
if args.task == 'q12':
    print("Top carriers")
    display_top('carrier_performance', 'carrier', 'arrival_delay', lambda x: x.arrival_delay, "%4s: %05.2f", str, float)
if args.task == 'q13':
    print("Weekday performance")
    display_top('weekday_performance', 'weekday', 'arrival_delay', lambda x: x.weekday, "%4s: %05.2f", str, float)
if args.task == 'q21':
    print("Enter origin code: ")
    origin = input().strip().upper()
    rows = simple_query('top_carriers_by_origin', 'origin', origin)
    display_list(rows, 'carriers', 'Top Carriers for %s' % origin)
elif args.task == 'q22':
    print("Enter origin code: ")
    origin = input().strip().upper()
    rows = simple_query('top_destinations_by_origin', 'origin', origin)
    display_list(rows, 'destinations', 'Top Destinations for %s' % origin)
elif args.task == 'q23':
    print("Enter origin and destinations codes: ")
    (origin, dest) = re.split('\s+', input().strip().upper())
    rows = simple_query('top_carriers_by_route', 'route', origin + '_' + dest)
    display_list(rows, 'carriers', 'Top Carriers for route %s -> %s' % (origin, dest))
elif args.task == 'q24':
    print("Enter origin and destinations codes: ")
    (origin, dest) = re.split('\s+', input().strip().upper())
    rows = simple_query('arrival_delay_by_route', 'route', origin + '_' + dest)
    display_val(rows, 'arrival_delay', '%0.2f', 'Mean Arrival Delay for route %s -> %s' % (origin, dest))
elif args.task == 'q32':
    print("Enter X Y Z YYYY-MM-DD: ")
    (x, y, z, dep_date_raw) = re.split('\s+', input().strip().upper())
    (year, m, d) = dep_date_raw.split('-')
    dd1 = datetime.date(int(year), int(m), int(d));
    dd2 = datetime.date(int(year), int(m), int(d) + 2);
    leg1 = flight_query(x, y, dd1, 0, 1200)
    leg2 = flight_query(y, z, dd2, 1200, 2400)
    display_flight(leg1)
    display_flight(leg2)

