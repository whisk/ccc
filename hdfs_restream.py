#!/usr/bin/python3

import subprocess
from subprocess import Popen
import hdfs
import time
import re
import sys

hdfs_path = '/ccc/input/'
hdfs_url  = 'http://hadoop-master:50070/'

hdfs_client = hdfs.client.InsecureClient(hdfs_url)

p = Popen(["hdfs", "dfs", "-ls", hdfs_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
code = p.wait()
if code != 0:
  print("Error with HDFS path %s: %s" % (hdfs_path, p.stderr.readlines()))
  sys.exit(1)

for line in p.stdout.readlines():
  cols = re.split(r'\s+', line.decode().strip())
  if len(cols) < 7:
    continue
  fname_curr = cols[7]
  fname_new = fname_curr
  m = re.search(r'___(\d{5})$', fname_curr)
  if m:
    num = int(m.group(1)) + 1
    fname_new = re.sub(r'___\d{5}', "___%05d" % num, fname_curr)
  else:
    fname_new = fname_curr + "___%05d" % 1

  if fname_curr != fname_new:
    print("Renameing %s -> %s" % (fname_curr, fname_new))
    hdfs_client.rename(fname_curr, fname_new)
    fname_curr = fname_new

  t = int(time.time()) * 1000
  print("Touching %s" % fname_curr)
  hdfs_client.set_times(fname_curr, access_time=t, modification_time=t)
  time.sleep(0.5)

