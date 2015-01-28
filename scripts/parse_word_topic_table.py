#!/usr/bin/python
import os, sys, random

if len(sys.argv) <> 5:
    print ''
    print 'Usage: python %s <vocab-file> <num-topic> <word-topic-table> <output-file>' % sys.argv[0]
    print ''
    sys.exit(1)

vocab_filename = sys.argv[1]
num_topic = int(sys.argv[2])
word_topic_filename = sys.argv[3]
output_filename = sys.argv[4]

vocab_array = []
with open(vocab_filename, 'r') as fvocab:
    for line in fvocab.readlines():
        words = line.strip().split()
        vocab_array.append(words[1])

topic_array = [[] for i in range(0, num_topic)]
with open(word_topic_filename, 'r') as fwt:
    for line in fwt.readlines():
        words = line.split()
        if len(words) > 0:
            word_id = int(words[0])
            for i in range(1, len(words)):
                topic_count = words[i].split(':')
                assert len(topic_count) == 2
                topic_id = int(topic_count[0])
                count = int(topic_count[1])
                topic_array[topic_id].append((vocab_array[word_id], count))
    topic_array_sorted = []
    for topic in topic_array:
        topic_sorted = sorted(topic, key=lambda t: -1*t[1])
        topic_array_sorted.append(topic_sorted)

with open(output_filename, 'w') as fout:
    for topic in topic_array_sorted:
        line = ''
        sum_item = 0.0
        for item in topic:
            sum_item = sum_item + item[1]
        for item in topic:
            line = line + item[0] + ':' + str(float(item[1])/sum_item) + '\t'
        line = line + '\n'
        fout.write(line)

