1, 
tf_df_filter_stop.voc
the 1 million vocabulary file of Wikipedia
each line consists of 3 columns separated by TAB, i.e., word \t term_frequency \t document_frequency
the lines are in descending order of term_frequency

2, dump_word_dict.py
a python script to generate binary and text dictionaries.

python dump_word_dict.py tf_df_filter_stop.voc  1000000
produces the following files:
word_dict.pickle
word_dict.pickle.txt
word_dict.id.txt

the file 'word_dict.id.txt' will be used by dump_dict_meta_sn.exe to produce the meta file describing a word being dense or sparse in word-topic table and alias table.





