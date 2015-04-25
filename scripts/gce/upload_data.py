#!/usr/bin/env python
# Upload data to gce servers
# Parameters
params = {
    # Input files:
    'libsvm_doc': '/home/yuntiand/daiwei/wrapup_preprocess/lightlda/datasets/20news.dat.conv' # Synced at Sat, 25 Apr 2015 15:47:35 GMT
    , 'word_tf_file': '/home/yuntiand/daiwei/wrapup_preprocess/lightlda/datasets/word_dict.id.txt.20news' # Synced at Sat, 25 Apr 2015 15:47:35 GMT

    # Block size:
    , 'block_size': 300

    # Ouput files:
    , 'binary_doc_dir': 'datasets/binary_doc_dir'
    , 'log_dirname': 'log'

    # Default params
    # Sync params in other files:
    , 'sync_params': True
    # File offset:
    , 'file_offset': 0
    # Mean document size (does not need to be accurate):
    , 'mean_doc_size': 100 # Synced at Sat, 25 Apr 2015 15:47:35 GMT
    # Tmp files directory:
    , 'tmp_directory': 'tmp'
}

###############################################################################
import sys, os, subprocess, time, codecs, re, shutil, glob

if __name__ == '__main__':
    # Figure out the paths
    script_path = os.path.realpath(__file__)
    script_dir = os.path.dirname(script_path)
    app_dir = os.path.dirname(script_dir)
    progname = 'dump_binary_doc_mn'
    prog_path = os.path.join(app_dir, 'bin', progname)

    # Judge input file exist and output directories not in temp directory and scripts not in temp directory
    assert os.path.isfile(params['libsvm_doc']), 'Libsvm doc file %s does not exist!' %params['libsvm_doc']
    assert os.path.isfile(params['word_tf_file']), 'Vocab file %s does not exist!' %params['word_tf_file']
    assert not os.path.realpath(params['binary_doc_dir']
            ).startswith(os.path.realpath(params['tmp_directory'])
                    ), 'Output file directory must not be the same as or in temp files directory (which will be deleted)'
    assert not  os.path.realpath(params['log_dirname']
            ).startswith(os.path.realpath(params['tmp_directory'])
                    ), 'Log file directory must not be the same as or in temp files directory (which will be deleted)'
    assert not script_path.startswith(os.path.realpath(params['tmp_directory'])
            ), 'Temp files directory must not contain scripts file (as temp files directory will be deleted)'

    # Create directories if not present
    print('Creating directories if not present')
    need_mkdirs = [params['tmp_directory'], params['binary_doc_dir'], params['log_dirname']]
    for need_mkdir in need_mkdirs:
        if not os.path.exists(need_mkdir):
            os.makedirs(need_mkdir)
    print('Creating done!')

    # Generate data blocks
    print('Generating data blocks')

    cmd = 'GLOG_logtostderr=False '
    cmd += 'GLOG_log_dir=%s ' %params['log_dirname']
    cmd += 'GLOG_v=-1 '
    cmd += 'GLOG_minloglevel=0 '
    cmd += 'GLOG_vmodule= '
    cmd += prog_path
    cmd += ' --undefok=' + ','.join('%s' %k for k in params) + ' '
    cmd += "".join([' --%s=%s' % (k,v) for k,v in params.items()])
    print(cmd)
    subprocess.call(cmd, shell=True)
    print('Generate data blocks done.')

    # Get max size of blocks
    block_sizes = [os.path.getsize(block_filename) for block_filename in glob.glob(os.path.join(params['binary_doc_dir'],'block*'))]
    max_block_size = max(block_sizes)

    # Sync params in other files:
    if params['sync_params']:
        # Sync run_lda.py 
        run_lda_path = os.path.join(script_dir, 'run_lda.py')
        tmp_run_lda_path = os.path.join(params['tmp_directory'], 'run_lda.py')
        assert os.path.isfile(run_lda_path), 'Sync failed, run_lda.py not found!'
        with codecs.open(run_lda_path, encoding='utf-8', mode='r') as fin:
            with codecs.open(tmp_run_lda_path, encoding='utf-8', mode='w') as ftmp:
                curr_timestamp = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
                for line in fin:
                    line_out = re.sub(ur"'word_id_file'\s*:.*", ur"'word_id_file': '%s' # Synced at %s" %(os.path.realpath(params['word_tf_file']), curr_timestamp), line)
                    line_out = re.sub(ur"'binary_doc_dir'\s*:.*", ur"'binary_doc_dir': '%s' # Synced at %s" %(os.path.realpath(params['binary_doc_dir']), curr_timestamp), line_out)
                    line_out = re.sub(ur"'block_size'\s*:.*", ur"'block_size': %d # Synced at %s" %(params['block_size']+1, curr_timestamp), line_out)
                    line_out = re.sub(ur"'block_max_capacity'\s*:.*", ur"'block_max_capacity': %d # Synced at %s" %(max_block_size, curr_timestamp), line_out)
                    line_out = re.sub(ur"'block_offset'\s*:.*", ur"'block_offset': %d # Synced at %s" %(params['file_offset'], curr_timestamp), line_out)
                    ftmp.write(line_out)
        if os.path.isfile(tmp_run_lda_path):
            os.rename(tmp_run_lda_path, run_lda_path)
        print('Parameters synced at %s' %curr_timestamp)

    # Delete temp directory
    shutil.rmtree(params['tmp_directory'])
