#!/usr/bin/python
# Count term frequency and document frequency in a libsvm formatted doc file
# Supports UTF-8 encoded vocabulary file
import os, sys, random
import codecs

def main(libsvm_filename, vocab_filename, output_filename, vocab_without_tf = True):
    vocab = []
    line_idx = 1
    with codecs.open(vocab_filename, encoding='utf-8', mode='r') as fvoc:
        for line in fvoc:
            if vocab_without_tf:
                vocab.append(line.strip())
            else:
                line_conv = line.strip().split()
                assert len(line_conv) >= 2, 'Line %d contains less than 2 terms' %line_idx
                vocab.append(line_conv[0])
            line_idx += 1
    
    tf_dict = {}
    df_dict = {}
    num_tokens = 0
    with open(libsvm_filename, 'r') as flib:
        with codecs.open(output_filename, encoding='utf-8', mode='w') as fout:
            line_idx = 1
            for line in flib.readlines():
                doc_tokens = 0
                words = line.strip().split()
                assert len(words) > 0, str(line_idx)
                flag_df = {}
                for i in range(1, len(words)):
                    word_count = words[i].split(':')
                    assert len(word_count) == 2
                    word_id = word_count[0]
                    count = int(word_count[1])
                    doc_tokens += count
                    if not word_id in tf_dict:
                        tf_dict[word_id] = count
                    else:
                        tf_dict[word_id] = tf_dict[word_id] + count
                    if not word_id in flag_df:
                        flag_df[word_id] = 1
                        if not word_id in df_dict:
                            df_dict[word_id] = 0
                        df_dict[word_id] = df_dict[word_id] + 1
                line_idx += 1
                num_tokens += doc_tokens if doc_tokens < 512 else 512
            mean_doc_size = float(num_tokens) / line_idx
            
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
            return mean_doc_size

if __name__ == '__main__':
    if len(sys.argv) not in range(4, 6):
        print >> sys.stderr, ''
        print >> sys.stderr, 'Usage: python %s <libsvm-filename> <vocab-filename> <output-filename> [<vocab-without-tf> = True]' % sys.argv[0]
        print >> sys.stderr, ''
        sys.exit(1)
    
    libsvm_filename = sys.argv[1]
    vocab_filename = sys.argv[2]
    output_filename = sys.argv[3]

    vocab_without_tf = True if len(sys.argv) < 5 else sys.argv[4] == 'True'

    main(libsvm_filename, vocab_filename, output_filename, vocab_without_tf)

