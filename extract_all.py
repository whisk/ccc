#!/usr/bin/env python

import subprocess
from subprocess import Popen
import re
import os

# data paths
dataset_path = '/dataset/aviation/airline_ontime/'
tmp_path = '/tmp/dataset-tmp'
hdfs_path = '/ccc/input/'

# extracting from csv
extractor_bin = './extractor.py'
extr_cols = [0, 2, 3, 4, 6, 11, 17, 23, 26, 37]

os.mkdir(tmp_path)

for zip_fname in Popen(['find', dataset_path, '-type', 'f', '-name', '*.zip'], stdout=subprocess.PIPE).stdout:
    zip_fname = zip_fname.strip()
    print "Found zip: %s" % (zip_fname)
    for line in Popen(['unzip', '-o', zip_fname, '-d', tmp_path], stdout=subprocess.PIPE).stdout:
        line = line.strip()
        m = re.match(r'^\s*inflating:\s+(.+\.csv)$', line)
        if m:
            csv_fname = m.group(1)
            extr_fname = csv_fname + '.txt'
            print "Extracting csv file %s to %s" % (csv_fname, extr_fname)
            Popen([extractor_bin, csv_fname, extr_fname] + [str(i) for i in extr_cols]).wait()

            print "Putting to HDFS"
            Popen(['hdfs', 'dfs', '-put', '-f', extr_fname, hdfs_path]).wait()
            print "OK"

            os.remove(csv_fname)
            os.remove(extr_fname)
        else:
            print "Skipping '%s'" % (line)

