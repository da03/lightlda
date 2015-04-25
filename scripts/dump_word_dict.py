#!/usr/bin/python
# Shuffle vocab such that word id and term frequencies are randomly distributed 
import sys
import codecs
import random
try:
    import cPickle as pickle
except:
    import pickle

def main(tf_df_filename, output_filename):
    vocab_size = 0
    with codecs.open(tf_df_filename, encoding='utf-8', mode='r') as ftfdf:
        for line in ftfdf:
            if len(line.strip()) > 0:
                vocab_size += 1
    id_list = range(vocab_size);
    random.seed(0)
    random.shuffle(id_list)
    
    with codecs.open(tf_df_filename, encoding='utf-8', mode='r') as ftfdf:
        word_dict = {}
        id2word = {}
        id2tf = {}
        word_num = 0
        for line in ftfdf:
            triple = line.split('\t')
            if len(triple) != 3:
                exit(1)
            word, tf, df = triple
            word_dict[word] = id_list[word_num]
            id2word[id_list[word_num]] = word
            id2tf[id_list[word_num]] = tf
            word_num += 1
        
        with codecs.open(output_filename, encoding='utf-8', mode='w') as handle:
            for id_item in range(word_num):
                handle.write(str(id_item) + '\t' + id2word[id_item] + '\t' + id2tf[id_item] + '\n');

if __name__ == '__main__':
    
    if len(sys.argv) != 3:
        print >> sys.stderr, 'Usage: python %s <tf-df-filename> <output-filename>' % sys.argv[0]
        sys.exit(1)
 

    tf_df_filename = sys.argv[1]
    output_filename = sys.argv[2]
    main(tf_df_filename, output_filename)
