import sys
import codecs
import random
try:
    import cPickle as pickle
except:
    import pickle

reader = codecs.getreader('utf-8')
writer = codecs.getwriter('utf-8')

sys.stdin = reader(sys.stdin)
sys.stdout = writer(sys.stdout)
sys.stderr = writer(sys.stderr)

if len(sys.argv) != 3:
    print >> sys.stderr, "Usage: python dump_pickle_dict.py word_dict_file vocabulary_size"
    exit(1)

id_list = range(0, int(sys.argv[2]));
random.seed(0)
random.shuffle(id_list)

word_dict_file = codecs.open(sys.argv[1], encoding='utf-8',mode='r')
word_dict = {}
id2word = {}
id2tf = {}
word_num = 0
for line in word_dict_file:
    triple = line.split('\t')
    if len(triple) != 3:
        exit(1)
    word, tf, df = triple
    word_dict[word] = id_list[word_num]
    id2word[id_list[word_num]] = word
    id2tf[id_list[word_num]] = tf
    word_num += 1


with open('word_dict.pickle', 'wb') as handle:
	pickle.dump(word_dict, handle)

with codecs.open('word_dict.pickle.txt', encoding='utf-8', mode='w') as handle:
    for key, value in word_dict.items():
        handle.write(key + '\t' + str(value) + '\n');

with codecs.open('word_dict.id.txt', encoding='utf-8', mode='w') as handle:
    for id_item in range(word_num):
        handle.write(str(id_item) + '\t' + id2word[id_item] + '\t' + id2tf[id_item] + '\n');
