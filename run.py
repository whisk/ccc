#!/usr/bin/env python

#
# Version: 1.0
# Usage: ./run.py [-n N] [-i hdfs_path] <TaskN ...>
# Requirements: hdfs, cassandra_import.py

import argparse
import sys
import datetime
import time
import subprocess
from subprocess import Popen

AVAIL_TASKS     = ['Task11', 'Task12', 'Task21', 'Task22', 'Task23', 'Task24', 'Task32']
CASSANDRA_HOSTS = ['10.0.223.125', '10.0.55.140']
CASSANDRA_TASKS = ['Task21', 'Task22', 'Task23', 'Task24']

parser = argparse.ArgumentParser(description='cmdline run tool for CCC Part 1')
parser.add_argument('tasks', nargs='+', choices=AVAIL_TASKS + ['all'],  help='Task Name')
parser.add_argument('-n',               default=10, type=int, help='N value')
parser.add_argument('-i', '--input',    default='/ccc/input', help='HDFS input path')
parser.add_argument('-o', '--output',   default='/ccc',       help='HDFS output path prefix')
#parser.add_argument('-k', '--keyspace', default='ccc_1',      help='Cassandra DB Keyspace')
#parser.add_argument('--host',           action='append',      help='Cassandra DB Host')
#parser.add_argument('-t', '--truncate', action='store_true',  default=True, help='Truncate before insertion')
args = parser.parse_args()

#if args.host == None:
#    args.host = CASSANDRA_HOSTS

if args.tasks == ['all']:
  args.tasks = AVAIL_TASKS

def run_cassandra_insert(task_name):
  print "Inserting to Cassandra for %s" % task_name
  p1 = Popen(['hdfs', 'dfs', '-cat', args.output + '/' + task_name + '/output/*'], stdout=subprocess.PIPE)
  p2 = Popen(['./cassandra_insert.py', '-t', task_name], stdin=p1.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  p2.wait()
  print "Done inserting for %s" % task_name

t1 = datetime.datetime.now()
processes   = {}
start_times = {}
for task_name in args.tasks:
  logfile_fname = "logs/%s-%s.log" % (task_name, t1.strftime('%Y%m%d%H%M%S'))
  logfile = open(logfile_fname, 'w')
  hdfs_input_path  = args.input
  hdfs_output_path = args.output + '/' + task_name + '/'
  sys.stdout.write("Starting %s...\n\tLogfile: %s\n\tOutput path: %s\n" % (task_name, logfile_fname, hdfs_output_path))
  # run hadoop task
  processes[task_name]   = Popen(['hadoop', 'jar', 'jars/%s.jar' % task_name, 'task.%s' % task_name, '-D', 'N=%d' % args.n, hdfs_input_path, hdfs_output_path], stdout=logfile, stderr=logfile)
  start_times[task_name] = time.time() 

proc_cnt = len(processes)
while True:
  for task_name in processes:
    if processes[task_name] == None:
      # skip finished processes 
      continue
    ret = processes[task_name].poll()
    if ret != None:
      # process has finished working
      sys.stdout.write("\n\nFinished %s\n\tExit code: %d\n\tRun time: %0.2fs\n\n" % (task_name, ret, time.time() - start_times[task_name]))
      processes[task_name] = None
      proc_cnt -= 1
      # now import result to cassandra
      if task_name in CASSANDRA_TASKS:
        ret = run_cassandra_insert(task_name)

  if proc_cnt == 0:
    # no running processes found
    break
  sys.stdout.write('.')
  sys.stdout.flush()
  time.sleep(5)
