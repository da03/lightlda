#!/usr/bin/python

# Convert libsvm such that all spaces are replaced with tab
import os, sys, random
import codecs

if len(sys.argv) <> 4:
    print ''
    print 'Usage: python %s <libsvm-filename> <vocab-filename> <output-filename>' % sys.argv[0]
    print ''
    sys.exit(1)

libsvm_filename = sys.argv[1]
vocab_filename = sys.argv[2]
output_filename = sys.argv[3]

vocab = []
with open(vocab_filename, 'r') as fvoc:
    for line in fvoc.readlines():
        line_conv = line.strip().split()
        assert len(line_conv) == 2
        vocab.append(line_conv[0])

tf_dict = {}
df_dict = {}
with open(libsvm_filename, 'r') as flib:
    with open(output_filename, 'w') as fout:
        line_idx = 1
        for line in flib.readlines():
            words = line.strip().split()
            assert len(words) > 0, str(line_idx)
            flag_df = {}
            for i in range(1, len(words)):
                word_count = words[i].split(':')
                assert len(word_count) == 2
                word_id = word_count[0]
                count = int(word_count[1])
                if not word_id in tf_dict:
                    tf_dict[word_id] = count
                else:
                    tf_dict[word_id] = tf_dict[word_id] + count
                if not word_id in flag_df:
                    flag_df[word_id] = 1
                    if not word_id in df_dict:
                        df_dict[word_id] = 0
                    df_dict[word_id] = df_dict[word_id] + 1
        
        word_triple_array = []
        for word_id in tf_dict:
            tf = tf_dict[word_id]
            if word_id in df_dict:
                df = df_dict[word_id]
            else:
                df = 0
            word_triple_array.append((vocab[int(word_id)], tf, df))
        word_array_sorted = sorted(word_triple_array, key=lambda t:-1*t[1])

        line_idx = 1
        for triple in word_array_sorted:
            word, tf, df = triple
            line_out = word + '\t' + str(tf) + '\t' + str(df)
            if line_idx > 1:
                line_out = '\n' + line_out
            line_idx = line_idx + 1
            fout.write(line_out)
