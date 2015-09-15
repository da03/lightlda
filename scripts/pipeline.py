#!/usr/bin/env python
# Spawn Petuum LightLDA instances
import sys, os, time, codecs
import generate_datablocks, run, utils


def main(params_file):
    params = {}
    params_file = os.path.realpath(params_file)
    print params_file
    # Figure out the paths
    script_path = os.path.realpath(__file__)
    script_dir = os.path.dirname(script_path)
    app_dir = os.path.dirname(script_dir)
    params['app_dir'] = app_dir
    default_params_path = os.path.join(app_dir, 'etc/default/params.default')
    assert os.path.isfile(default_params_path), 'Default parameters file %s is missing!' %default_params_path

    # Current timestamp
    curr_timestamp = time.strftime("%d_%b_%Y_%H_%M_%S_GMT", time.gmtime())

    # read configuration file and check
    utils.read_config(default_params_path, params)
    utils.read_config(params_file, params)
    #print params
    #check_config(params)
    input_dir = params['input_dir']
    assert os.path.isdir(input_dir), 'inputdir %s is not a valid directory!' %input_dir
    params['input_dir'] = os.path.realpath(input_dir)
    output_dir = params['output_dir']
    if not os.path.exists(output_dir):
        utils.mkdir_p(output_dir)
    params['output_dir'] = os.path.realpath(output_dir)
    log_dir = os.path.join(params['log_dir'], curr_timestamp)
    if not os.path.exists(log_dir):
        utils.mkdir_p(log_dir)
    params['log_dir'] = os.path.realpath(log_dir)
    vocab_stopword_file  = params['vocab_stopword']
    if vocab_stopword_file == '_DEFAULT_':
        vocab_stopword_file = os.path.join(app_dir, 'etc/stopword')
    assert os.path.isfile(vocab_stopword_file), 'vocab_stopword_file %s is not a valid file!' %vocab_stopword_file
    params['vocab_stopword'] = vocab_stopword_file

    # read directory, generate data blocks
    datablocks_dir = os.path.join(output_dir, 'datablocks', curr_timestamp)
    if not os.path.exists(datablocks_dir):
        utils.mkdir_p(datablocks_dir)
    params['datablocks_dir'] = os.path.realpath(datablocks_dir)
    generate_datablocks.generate_from_dir(params)

    # generate meta information and run
    if params['SSH_identity_file'] == '_NO_':
        params.pop('SSH_identity_file', None)
    else:
        ssh_identity_file = params['SSH_identity_file']
        assert os.path.isfile(ssh_identity_file), 'SSH_identity_file %s is not a valid file!' %ssh_identity_file
        params['SSH_identity_file'] = os.path.realpath(ssh_identity_file)
    if params['SSH_user_name'] == '_NO_':
        params.pop('SSH_user_name', None)
    word_id_file = os.path.join(datablocks_dir, 'word_tf.txt')
    assert os.path.isfile(word_id_file), 'word_tf.txt is not generated successfully by generate_datablocks!'
    params['word_id_file'] = os.path.realpath(word_id_file)
    params['dict_meta_file'] = os.path.join(os.path.realpath(datablocks_dir), 'dict_meta')
    if params['cold_start'] == 'True' or params['cold_start'] == 'true':
        params['cold_start'] = True
    else:
        params['cold_start'] = False
    dump_dir = os.path.join(output_dir, 'model', curr_timestamp)
    if not os.path.exists(dump_dir):
        utils.mkdir_p(dump_dir)
    params['dump_file'] = os.path.join(os.path.realpath(dump_dir), 'snapshot')
    host_file = params['host_file']
    assert os.path.isfile(host_file), 'host_file %s is not a valid file!' %host_fle
    params['host_file'] = os.path.realpath(host_file)
    run.run(params)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print >> sys.stderr, 'Usage: python %s <params-file>' %sys.argv[0]
        sys.exit(1)
    params_file = sys.argv[1]
    assert os.path.isfile(params_file), 'params-file %s is not a valid file!' %params_file
    main(params_file)
