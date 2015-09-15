#!/usr/bin/python
import os, sys, random, codecs

if len(sys.argv) <> 5:
    print >> sys.stderr, ''
    print >> sys.stderr, 'Usage: python %s <vocab-file> <num-topic> <word-topic-tables> <output-file>' % sys.argv[0]
    print >> sys.stderr, ''
    sys.exit(1)

vocab_filename = sys.argv[1]
num_topic = int(sys.argv[2])
word_topic_filename = sys.argv[3]
output_filename = sys.argv[4]

vocab_array = []
with codecs.open(vocab_filename) as fvocab:
    for line in fvocab:
        words = line.strip().split()
        vocab_array.append(words[1])

topic_array = [[] for i in range(0, num_topic)]
word_topic_tables = word_topic_filename.split(':')
topic_array_sorted = []
line_idx = 0
for word_topic_table in word_topic_tables:
    with open(word_topic_table, 'r') as fwt:
        print 'Parsing ', word_topic_table
        for line in fwt:
            line_idx = line_idx + 1
            if line_idx % 100000 == 0:
                print line_idx, 'lines processed'
            words = line.split()
            if len(words) > 0:
                word_id = int(words[0])
                for i in range(1, len(words)):
                    topic_count = words[i].split(':')
                    assert len(topic_count) == 2
                    topic_id = int(topic_count[0])
                    count = int(topic_count[1])
                    topic_array[topic_id].append((vocab_array[word_id], count))
for topic in topic_array:
    topic_sorted = sorted(topic, key=lambda t: -1*t[1])
    topic_array_sorted.append(topic_sorted)

with codecs.open(output_filename, mode='w') as fout:
    for topic in topic_array_sorted:
        line = ''
        sum_item = 0.0
        for item in topic:
            sum_item = sum_item + item[1]
        for item in topic:
            line = line + item[0] + ':' + str(float(item[1])/sum_item) + '\t'
        line = line + '\n'
        fout.write(line)

