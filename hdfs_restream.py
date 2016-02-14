#!/usr/bin/python3

import hdfs
import time
import re
import sys

hdfs_path = '/ccc/input/'
hdfs_url  = 'http://hadoop-master:50070/'

hdfs_client = hdfs.client.InsecureClient(hdfs_url)
for fname in hdfs_client.list(hdfs_path):
  fname_curr = hdfs_path + fname
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
  time.sleep(1)
