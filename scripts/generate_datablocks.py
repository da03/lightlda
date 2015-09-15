#!/usr/bin/env python
# Generate data blocks
###############################################################################
import sys, os, subprocess, time, codecs, re, shutil, glob

def generate_from_dir(params):
    input_dir = params['input_dir']
    assert os.path.isdir(input_dir), 'input_dir %s is not a valid directory!' %input_dir
    files_or_dirs = glob.glob(os.path.join(input_dir, '*'))
    files_or_dirs = glob.glob('%s/*' %input_dir)
    input_files = [file_or_dir for file_or_dir in files_or_dirs if os.path.isfile(file_or_dir)]
    assert len(input_files) > 0, 'input_dir %s does not contain any file!' %input_dir

    params_local = {}
    params_local['input_dir'] = input_dir
    params_local['vocab_stopword'] = params['vocab_stopword']
    params_local['vocab_min_occurence'] = params['vocab_min_occurence']
    params_local['datablocks_dir'] = params['datablocks_dir']
    params_local['block_size'] = params['block_size']
    params_local['mean_doc_size'] = params['mean_doc_size']
    progname = 'generate_datablocks'
    app_dir = params['app_dir']
    prog_path = os.path.join(app_dir, 'bin', progname)
    # Generate data blocks
    print('Generating data blocks')
    cmd = 'GLOG_logtostderr=False '
    cmd += 'GLOG_log_dir=%s ' %params['log_dir']
    cmd += 'GLOG_v=-1 '
    cmd += 'GLOG_minloglevel=0 '
    cmd += 'GLOG_vmodule= '
    cmd += prog_path
    #cmd += ' --undefok=' + ','.join('%s' %k for k in params) + ' '
    cmd += "".join([' --%s=%s' % (k,v) for k,v in params_local.items()])
    print(cmd)
    subprocess.call(cmd, shell=True)
    print('Generate data blocks done.')
