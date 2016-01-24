#!/usr/bin/env python

# A a script to print specified fields from csv by number (starting from zero)
# Version: 0.1
# Usage: ./extractor.py file.csv 4 6 37

import sys
import csv

fname = sys.argv[1]
f = open(fname, 'r')
csv_reader = csv.reader(f, delimiter=',', quotechar='"')

fields = sys.argv[2:]

header = csv_reader.next()
for row in csv_reader:
    for i in fields:
        print (row[int(i)]), 
    print
