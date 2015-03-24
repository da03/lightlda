#!/usr/bin/python

# extract the time and llh in log file
import os, sys, re

if len(sys.argv) <> 3:
    print ''
    print 'Usage: python %s <log-filename> <output-filename>' % sys.argv[0]
    print ''
    sys.exit(1)

log_filename = sys.argv[1]
output_filename = sys.argv[2]

with open(log_filename, 'r') as fin:
    with open(output_filename, 'w') as fout:
        iter = -1
        elapsed_time = -1.0
        doc_llh = -1.0
        word_llh = -1.0
        total_llh = -1.0
        for line in fin.readlines():
            m = re.search(r'Iter: (\d+) Elapsed_wait time = .*?Elapsed time = ([\d\.]+)', line)
            if m:
                assert iter == -1 or iter == (int(m.group(1)) - 1)
                iter = int(m.group(1))
                elapsed_time = float(m.group(2))
            m = re.search(r'Doc likelihood = ([-e\d\.\+]+) Word Likelihood = ([-e\d\.\+]+)', line)
            if m:
                doc_llh = float(m.group(1))
                word_llh = float(m.group(2))
                total_llh = doc_llh + word_llh
                line_out = '%f\t%f\n' % (elapsed_time, total_llh)
                fout.write(line_out)
