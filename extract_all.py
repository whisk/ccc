#!/usr/bin/env python

import subprocess
from subprocess import Popen
import re
import os
import sys
import errno
import time
import threading

# data paths
dataset_path = '/dataset/aviation/airline_ontime/'
tmp_path = '/tmp/dataset-tmp'
hdfs_path = '/ccc/input/'
threads_count = 2

# extracting from csv
extractor_bin = './extractor.py'
extr_cols = [0, 2, 3, 4, 6, 11, 17, 23, 26, 37]

try:
    os.mkdir(tmp_path)
except OSError as e:
    if e.errno != errno.EEXIST:
        sys.exit(1)

t1 = time.time()
code = Popen(["hdfs", "dfs", "-mkdir", "-p", hdfs_path]).wait()
if code != 0:
    print "Error with HDFS path %s" % hdfs_path
    sys.exit(1)

zip_fnames = Popen(['find', dataset_path, '-type', 'f', '-name', '*.zip'], stdout=subprocess.PIPE).stdout.readlines()
print "Found %d zip files" % len(zip_fnames)

def unzipper(zip_fnames, result, idx):
    sum_raw_size = 0.0
    sum_extr_size = 0.0
    sum_lines = 0
    for zip_fname in zip_fnames:
        i += 1
        zip_fname = zip_fname.strip()
        print "Found zip: %s" % (zip_fname)
        for line in Popen(['unzip', '-o', zip_fname, '-d', tmp_path], stdout=subprocess.PIPE).stdout:
            line = line.strip()
            m = re.match(r'^\s*inflating:\s+(.+\.csv)$', line)
            if m:
                csv_fname = m.group(1)
                extr_fname = csv_fname + '.txt'
                sum_raw_size += os.stat(csv_fname).st_size
                print "Extracting csv file %s to %s" % (csv_fname, extr_fname)
                code = Popen([extractor_bin, csv_fname, extr_fname] + [str(c) for c in extr_cols]).wait()
                if code != 0:
                    print "Error extracting %s" % csv_fname
                    continue

                sum_extr_size += os.stat(extr_fname).st_size
                sum_lines += int(Popen(['wc', '-l', extr_fname], stdout=subprocess.PIPE).stdout.readlines()[0].split(' ')[0])

                print "Putting %s to HDFS://%s" % (extr_fname, hdfs_path)
                code = Popen(['hdfs', 'dfs', '-put', '-f', extr_fname, hdfs_path]).wait()
                if code != 0:
                    print "Error putting to HDFS"
                    continue

                print "OK"

                os.remove(csv_fname)
                os.remove(extr_fname)
            else:
                print "Skipping '%s'" % (line)
    result[idx] = (sum_raw_size, sum_extr_size, sum_lines)

threads = [None] * threads_count
res     = [None] * threads_count
for t in range(threads_count):
    threads[t] = threading.Thread(target=unzipper, args=(zip_fnames[t::threads_count], res, t))
    threads[t].start()

for thread in threads:
    thread.join()

print "Raw size: %0.2fG" % (sum(x[0] for x in res) / 1024 / 1024 / 1024)
print "Extracted size: %0.2fG" % (sum(x[1] for x in res) / 1024 / 1024 / 1024)
print "Total lines: %d" % sum(x[2] for x in res)
print "Total time: %0.2fs" % (time.time() - t1)
