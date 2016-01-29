#!/usr/bin/env python

# A script to print specified fields from csv by number (starting from zero)
# Version: 0.1
# Usage: ./extractor.py file_in.csv file_out.txt 4 6 37

import sys
import csv

file_in = open(sys.argv[1], 'r')
csv_reader = csv.reader(file_in, delimiter=',', quotechar='"')
file_out = open(sys.argv[2], 'w')
fields = sys.argv[3:]

header = csv_reader.next()
l = 0
for row in csv_reader:
    l += 1
    if l == 1:
        continue
    for i in fields:
        file_out.write(row[int(i)] + ' ') 
    file_out.write("\n")
