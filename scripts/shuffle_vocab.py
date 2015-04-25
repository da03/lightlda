#!/usr/bin/env python
# Shuffle vocabulary of libsvm formatted doc file, output converted doc file and vocab
# Parameters
params = {
    # Input files:
    'vocab_filename': 'datasets/20news.vocab'
    , 'vocab_without_tf': True
    , 'doc_filename': 'datasets/20news.dat.tab'

    # Ouput files:
    , 'word_id_filename': 'datasets/word_dict.id.txt.20news'
    , 'libsvm_doc': 'datasets/20news.dat.conv'

    # Default params
    # Sync params in other files:
    , 'sync_params': True
    # Tmp files directory:
    , 'tmp_directory': 'tmp'
}

###############################################################################
import sys, os, subprocess, time, shutil, codecs, re, math
import count_tf_df, dump_word_dict, convert_word_id

if __name__ == '__main__':
    # Figure out the paths
    script_path = os.path.realpath(__file__)
    script_dir = os.path.dirname(script_path)
    app_dir = os.path.dirname(script_dir)

    # Judge input files exist
    assert os.path.isfile(params['vocab_filename']), 'Vocab file %s does not exist!' %params['vocab_filename']
    assert os.path.isfile(params['doc_filename']), 'Doc file %s does not exist!' %params['doc_filename']

    # Create directories if not present
    print('Creating directories if not present')
    need_mkdirs = [params['tmp_directory'], os.path.dirname(params['word_id_filename']), os.path.dirname(params['libsvm_doc'])]
    for need_mkdir in need_mkdirs:
        if not os.path.exists(need_mkdir):
            os.makedirs(need_mkdir)
    print('Creating done!')

    tmp_tf_df_filename = os.path.join(params['tmp_directory'], 'tf_df')
    # Count term frequency and document frequency
    print('Counting term frequency and document frequency')
    mean_doc_size = count_tf_df.main(params['doc_filename'], params['vocab_filename'], tmp_tf_df_filename, params['vocab_without_tf'])
    print('Counting done.')
    # Shuffle vocab and generate new vocab id file
    print('Shuffling and generating new vocab file')
    dump_word_dict.main(tmp_tf_df_filename, params['word_id_filename'])
    print('Shuffling done.')
    # Convert word id
    print('Converting word id')
    convert_word_id.main(params['doc_filename'], params['vocab_filename'], params['word_id_filename'], params['libsvm_doc'], params['vocab_without_tf'])
    print('Converting done.')

    # Sync params in other files:
    if params['sync_params']:
        # Judge output directories not in temp directory and scripts not in temp directory
        assert not os.path.realpath(os.path.dirname(params['word_id_filename'])
            ).startswith(os.path.realpath(params['tmp_directory'])
                ), 'Output file directory must not be the same as or in temp files directory (which will be deleted)'
        assert not  os.path.realpath(os.path.dirname(params['libsvm_doc'])
            ).startswith(os.path.realpath(params['tmp_directory'])
                ), 'Output file directory must not be the same as or in temp files directory (which will be deleted)'
        assert not script_path.startswith(os.path.realpath(params['tmp_directory'])
            ), 'Temp files directory must not contain scripts file (as temp files directory will be deleted)'
        # Sync generate_data_blocks.py
        generate_data_blocks_path = os.path.join(script_dir, 'generate_data_blocks.py')
        tmp_data_blocks_path = os.path.join(params['tmp_directory'], 'generate_data_blocks.py')
        assert os.path.isfile(generate_data_blocks_path), 'Sync failed, generate_data_blocks.py not found!'
        with codecs.open(generate_data_blocks_path, encoding='utf-8', mode='r') as fin:
            with codecs.open(tmp_data_blocks_path, encoding='utf-8', mode='w') as ftmp:
                curr_timestamp = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
                for line in fin:
                    line_out = re.sub(ur"'libsvm_doc'\s*:.*", ur"'libsvm_doc': '%s' # Synced at %s" %(os.path.realpath(params['libsvm_doc']), curr_timestamp), line)
                    line_out = re.sub(ur"'word_tf_file'\s*:.*", ur"'word_tf_file': '%s' # Synced at %s" %(os.path.realpath(params['word_id_filename']), curr_timestamp), line_out)
                    line_out = re.sub(ur"'mean_doc_size'\s*:.*", ur"'mean_doc_size': %d # Synced at %s" %(math.ceil(mean_doc_size), curr_timestamp), line_out)
                    ftmp.write(line_out)
        if os.path.isfile(tmp_data_blocks_path):
            os.rename(tmp_data_blocks_path, generate_data_blocks_path)
        print('Parameters synced at %s' %curr_timestamp)

    # Delete temp directory
    shutil.rmtree(params['tmp_directory'])
