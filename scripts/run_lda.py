#!/usr/bin/env python
# Parameters
params = {
    'shared_file_system': True

    # Input files:
    , 'word_id_file': '/home/yuntiand/daiwei/wrapup_preprocess/lightlda/datasets/word_dict.id.txt.20news' # Synced at Sat, 25 Apr 2015 15:48:05 GMT
    , 'host_filename': 'scripts/localserver'
    , 'binary_doc_dir': '/home/yuntiand/daiwei/wrapup_preprocess/lightlda/datasets/binary_doc_dir' # Synced at Sat, 25 Apr 2015 15:48:05 GMT

    # LDA Parameters:
    , 'num_topics': 100
    , 'alpha': 0.1
    , 'beta': 0.01

    # Output files:
    , 'dict_meta_file': 'datasets/meta'
    , 'output_dirname': 'output'
    , 'log_dirname': 'log'
    , 'dump_file': 'output/dump'

    # Execution parameters:
    , 'cold_start': True
    , 'num_iterations': 1000
    , 'block_max_capacity': 435052 # Synced at Sat, 25 Apr 2015 15:48:05 GMT
    , 'model_max_capacity': 2000000
    , 'alias_max_capacity': 2000000
    , 'delta_max_capacity': 200000
    , 'compute_ll_interval': 1
    , 'dump_model_interval': 4
    , 'block_size': 301 # Synced at Sat, 25 Apr 2015 15:48:05 GMT

    # System parameters:
    , 'num_worker_threads': 16
    , 'num_delta_threads': 2
    , 'staleness': 1

    # Default parameters:
    , 'block_offset': 0 # Synced at Sat, 25 Apr 2015 15:48:05 GMT
    , 'mh_step': 2
    , 'load_factor': 2
}

###############################################################################
# Spawn Petuum Sparse Coding on hosts listed in hostfile
import sys, os, subprocess, time, glob, codecs, re

if __name__ == '__main__':
    # Figure out the paths
    script_path = os.path.realpath(__file__)
    script_dir = os.path.dirname(script_path)
    app_dir = os.path.dirname(script_dir)
    log_path = os.path.realpath(params['log_dirname'])
    output_path = os.path.realpath(params['output_dirname'])

    # Parse hostfile
    host_file = os.path.realpath(params['host_filename'])
    try:
        fhost = open(host_file)
        host_list = [x.split()[1] for x in fhost.readlines()]
        unique_host_list = []
        for host in host_list:
            if host not in unique_host_list:
                unique_host_list.append(host)
        num_hosts = len(unique_host_list)
        fhost.close()
    except:
        print('Error: Cannot read host file %s' %params['host_filename'])
        sys.exit(1)


    # Count vocab size
    try: 
        fvoc = codecs.open(params['word_id_file'], encoding='utf-8', mode='r')
        vocab_size = 0
        for line in fvoc:
            if len(line.strip()) > 0:
                vocab_size += 1
    except:
        print('Error: Cannot read word id file %s' %params['word_id_file'])
        sys.exit(1)

    # Set parameters
    params['num_clients'] = num_hosts
    params['num_vocabs'] = vocab_size
    params['hostfile'] = host_file
    params['doc_file'] = os.path.join(os.path.realpath(params['binary_doc_dir']), 'block')
    params['vocab_file'] = os.path.join(os.path.realpath(params['binary_doc_dir']), 'vocab')
    params['dump_file'] = os.path.realpath(params['dump_file'])
    params['dict_meta_file'] = os.path.realpath(params['dict_meta_file'])

    # SSH options
    ssh_options = (' -oStrictHostKeyChecking=no'
        ' -oUserKnownHostsFile=/dev/null'
        ' -oLogLevel=quiet'
    )

    # Create output and log directory if not present
    print('Creating output and log directory if not present on clients')
    need_mkdirs = [output_path, log_path, os.path.realpath(os.path.dirname(params['dump_file'])), os.path.realpath(os.path.dirname(params['dict_meta_file']))]
    for need_mkdir in need_mkdirs:
        for ip in unique_host_list:
            cmd = ' '.join(['ssh', ssh_options, ip, 'test -d %s || mkdir -p %s' %(need_mkdir, need_mkdir)])
            subprocess.call(cmd, shell=True)
    time.sleep(1)
    print('Creating done!')

    # Generate vocab meta info
    print('Generating vocab meta info for %d hosts' %num_hosts)
    progname = 'dump_dict_meta_mn'
    prog_path = os.path.join(app_dir, 'bin', progname)
    client_id = 0
    for ip in unique_host_list:
        cmd = 'ssh %s %s ' %(ssh_options, ip)
        cmd += 'GLOG_logtostderr=False '
        cmd += 'GLOG_log_dir=%s ' %log_path
        cmd += 'GLOG_v=-1 '
        cmd += 'GLOG_minloglevel=0 '
        cmd += 'GLOG_vmodule= '
        cmd += prog_path
        cmd += ' --undefok=' + ','.join('%s' %k for k in params) + ' '
        cmd += ''.join([' --%s=%s' % (k,v) for k,v in params.items()])
        if client_id == 0:
            print(cmd)
        subprocess.call(cmd, shell=True)
        if params['shared_file_system']:
            break
        client_id += 1
    time.sleep(1)
    print('Vocab meta info generated')
    
    progname = 'lda_main'
    prog_path = os.path.join(app_dir, 'bin', progname)

    # Kill previous instances of this program
    print('Killing previous instances of %s on servers, please wait...' % progname)
    for ip in unique_host_list:
        cmd = ' '.join(['ssh', ssh_options, ip, 'killall -q %s' %progname])
        subprocess.call(cmd, shell=True)
    print('All killed!')

    # Spawn program instances
    print('Spawning program instances')
    client_id = 0
    block_offset = params['block_offset']
    client_block_offset = block_offset
    local_num_blocks = len(glob.glob(params['doc_file']+'*'))
    assert local_num_blocks > 0, 'No blocks in %s found!' %params['doc_file']
    if params['shared_file_system']:
        num_blocks = local_num_blocks
        ave_block_num = num_blocks / num_hosts
    else:
        num_blocks_by_ip = {}
        block_offset_by_ip = {}
        for ip in unique_host_list:
            cmd = ' '.join(['ssh', ssh_options, ip, 'ls -l %s*' %(os.path.realpath(params['doc_file']))])
            try:
                block_file_list_raw = subprocess.check_output(cmd, shell=True).split('\n')
                block_file_list = [block_filename for block_filename in block_file_list_raw if len(block_filename.strip()) > 0]

                block_id_list = [int(re.match(r'.*block\.(\d+)', block_filename).group(1)) for block_filename in block_file_list]
                block_offset_by_ip[ip] = min(block_id_list)
                num_blocks_by_ip[ip] = len(block_id_list)
            except:
                print >> sys.stderr, 'Cannot get number of blocks on host %s' %ip
                sys.exit(1)
    for ip in unique_host_list:
        print('Running client %d on %s' %(client_id, ip))
        # Set client specific parameters
        params['meta_name'] = '%s.%d' %(os.path.realpath(params['dict_meta_file']), client_id)
        params['client_id'] = client_id
        # Set number of blocks
        if params['shared_file_system']:
            if client_block_offset + ave_block_num > block_offset + num_blocks:
                client_num_blocks = num_blocks - client_block_offset
            else:
                if client_id == num_hosts - 1:
                    client_num_blocks = num_blocks - client_block_offset
                else:
                    client_num_blocks = ave_block_num
            params['num_blocks'] = client_num_blocks
            params['block_offset'] = client_block_offset
            client_block_offset += client_num_blocks
        else:
            params['num_blocks'] = num_blocks_by_ip[ip]
            params['block_offset'] = block_offset_by_ip[ip]

        cmd = 'ssh %s %s ' %(ssh_options, ip)
        cmd += 'GLOG_logtostderr=False '
        cmd += 'GLOG_log_dir=%s ' %log_path
        cmd += 'GLOG_v=-1 '
        cmd += 'GLOG_minloglevel=0 '
        cmd += 'GLOG_vmodule= '
        cmd += prog_path
        cmd += ' --undefok=' + ','.join('%s' %k for k in params) + ' '
        cmd += ''.join([' --%s=%s' % (k,v) for k,v in params.items()])
        cmd += '&'
        subprocess.call(cmd, shell=True)
        
        # Wait a few seconds for the name node (client 0) to set up
        print cmd
        if client_id == 0:
          print(cmd)   # print the cmd for just the first machine.
          print 'Waiting for name node to set up...'
          time.sleep(3)
        client_id += 1
