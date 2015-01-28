#!/usr/bin/python

# Convert libsvm such that all spaces are replaced with tab
import os, sys, random

if len(sys.argv) <> 3:
    print ''
    print 'Usage: python %s <libsvm-filename> <output-filename>' % sys.argv[0]
    print ''
    sys.exit(1)

libsvm_filename = sys.argv[1]
output_filename = sys.argv[2]
with open(libsvm_filename, 'r') as flib:
    with open(output_filename, 'w') as fout:
        line_idx = 1
        for line in flib.readlines():
            words = line.strip().split()
            assert len(words) > 0, str(line_idx)
            line_out = '\t'.join(words)
            if line_idx > 1:
                line_out = '\n' + line_out
            line_idx = line_idx + 1
            fout.write(line_out)
