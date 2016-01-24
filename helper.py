#!/usr/bin/env python

# A a script to print csv field names and sample values (1st and 2nd rows actually)
# Version: 0.1
# Usage: ./helper.py file.csv

import sys
import csv

fname = sys.argv[1]
f = open(fname, 'r')
csv_reader = csv.reader(f, delimiter=',', quotechar='"')

header = csv_reader.next()
data = csv_reader.next()

for i in range(len(header)):
    print "%02d '%s' '%s'" % (i, header[i], data[i])
