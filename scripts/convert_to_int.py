#!/usr/bin/python

# Convert word id to meet dictionary
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
        for line in flib:
            words = line.strip().split()
            assert len(words) > 0, str(line_idx)
            words_conv = []
            for i in range(1,len(words)):
                words_conv.append(words[i])
            line_out = '0' + '\t' + '\t'.join(words_conv)
            if line_idx > 1:
                line_out = '\n' + line_out
            if line_idx % 10000 == 0:
                print line_idx
            line_idx = line_idx + 1
            fout.write(line_out)
