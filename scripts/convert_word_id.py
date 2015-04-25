#!/usr/bin/python

# Convert word id to meet dictionary
import os, sys, random, codecs

def main(libsvm_filename, vocab_filename, wordid_filename, output_filename, vocab_without_tf=True):
    voc_dict = {}
    with codecs.open(vocab_filename, encoding='utf-8', mode='r') as fvoc:
        line_idx = 0
        for line in fvoc:
            if vocab_without_tf:
                voc_dict[line.strip()] = str(line_idx)
            else:
                line_conv = line.strip().split()
                assert len(line_conv) == 2, str(line_idx+1)
                voc_dict[line_conv[0]] = str(line_idx)
            line_idx = line_idx + 1
    
    newvoc_dict = {}
    with codecs.open(wordid_filename, encoding='utf-8', mode='r') as fnew:
        line_idx = 0
        for line in fnew:
            words = line.strip().split()
            assert len(words) == 3, str(line_idx+1)
            line_idx = line_idx + 1
            wordid, word, tf = words 
            newvoc_dict[word] = wordid
    
    map_dict = {}
    for word in voc_dict:
        #assert word in newvoc_dict, word
        if word in newvoc_dict:
            map_dict[voc_dict[word]] = newvoc_dict[word]
    
    with open(libsvm_filename, 'r') as flib:
        with open(output_filename, 'w') as fout:
            line_idx = 1
            for line in flib:
                words = line.strip().split()
                assert len(words) > 0, str(line_idx)
                words_conv = []
                for i in range(1, len(words)):
                    word = words[i]
                    word_count = word.split(':')
                    assert len(word_count) == 2, (str(line_idx) + word)
                    wordid = word_count[0]
                    count = word_count[1]
                    assert wordid in map_dict, (str(line_idx) + ' ' + wordid)
                    words_conv.append((map_dict[wordid] + ':' + count))
                line_out ='0' + '\t' + '\t'.join(words_conv)
                if line_idx > 1:
                    line_out = '\n' + line_out
                line_idx = line_idx + 1
                fout.write(line_out)

if __name__ == '__main__':
    if len(sys.argv) not in range(5, 7):
        print >> sys.stderr, ''
        print >> sys.stderr, 'Usage: python %s <libsvm-filename> <vocab-filename> <wordid-filename> <output-filename> [<vocab-without-tf>=True]' % sys.argv[0]
        print >> sys.stderr, ''
        sys.exit(1)
    
    libsvm_filename = sys.argv[1]
    vocab_filename = sys.argv[2]
    wordid_filename = sys.argv[3]
    output_filename = sys.argv[4]

    vocab_without_tf = sys.argv[5]=='True' if len(sys.artv) > 5 else False
    
    main(libsvm_filename, vocab_filename, wordid_filename, output_filename, vocab_without_tf)
