#!/usr/bin/env python
# Spawn Petuum LightLDA instances
# Parameters
params = {
    'shared_file_system': False # if the remote server is shared or not file system
    , 'copy_files': True

    # Input files:
    , 'word_id_file': '/home/yuntiand/daiwei/wrapup_preprocess/lightlda/datasets/word_dict.id.txt.20news' # Synced at Sat, 25 Apr 2015 15:48:05 GMT, if copy_files set True, then must exist locally; else exist remotely
    , 'host_file': '/home/yuntiand/daiwei/wrapup_preprocess/update7/lightlda/machinefiles/gce_hosts' # Synced at Tue, 28 Apr 2015 02:24:37 GMT
    , 'binary_doc_dir': '/home/yuntiand/daiwei/wrapup_preprocess/lightlda/datasets/binary_doc_dir' # Synced at Sat, 25 Apr 2015 15:48:05 GMT, if copy_files set True, then must exist locally; else exist remotely

    # LDA Parameters:
    , 'num_topics': 100
    , 'alpha': 0.1
    , 'beta': 0.01

    # Output files:
    , 'dict_meta_file': 'datasets/meta' # must be created remotely, path relative to working dir or app dir or absolute path
    , 'log_dir': 'log' # must be created remotely
    , 'dump_file': 'output/dump' # must be created remotely

    # Execution parameters:
    , 'cold_start': True
    , 'num_iterations': 100
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
    , 'ssh_identity_file': '~/.ssh/google_compute_engine' # Synced at Tue, 28 Apr 2015 02:24:37 GMT
    , 'ssh_username': 'lightlda' # Synced at Tue, 28 Apr 2015 02:24:37 GMT
    , 'internal_host_file': '/home/yuntiand/daiwei/wrapup_preprocess/update7/lightlda/machinefiles/gce_internal_hosts' # Synced at Tue, 28 Apr 2015 02:24:37 GMT
    , 'remote_app_dir': '~/lightlda' # Synced at Tue, 28 Apr 2015 02:24:37 GMT

    # Default parameters:
    , 'block_offset': 0 # Synced at Sat, 25 Apr 2015 15:48:05 GMT
    , 'mh_step': 2
    , 'load_factor': 2

    # Sync params in other files:
    , 'sync_params': True
    # Tmp files directory:
    , 'tmp_directory': 'tmp'
}

###############################################################################
import sys, os, subprocess, time, glob, codecs, re, multiprocessing, shutil
def replace_tilde_path(path_names, remote_home_dir):
    for path_name in path_names:
        m = re.match(r'~/(.*)', params[path_name])
        if m:
            params[path_name] = os.path.join(remote_home_dir, m.group(1))
        m = re.match(r'~', params[path_name])
        if m:
            params[path_name] = remote_home_dir

def exec_cmds(cmds):
    for cmd in cmds:
        subprocess.call(cmd, shell=True)

def multiprocess_exec_cmds(cmds):
    cmds_ip_dict = {}
    for cmd in cmds:
        ip = re.search(r'(\d+\.\d+\.\d+\.\d+)', cmd).group(1)
        if ip not in cmds_ip_dict:
            cmds_ip_dict[ip] = []
        cmds_ip_dict[ip].append(cmd)
    threads = []
    for ip in cmds_ip_dict:
        thread = multiprocessing.Process(target=exec_cmds, args=(cmds_ip_dict[ip], ))
        threads.append(thread)
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    time.sleep(1)
if __name__ == '__main__':
    # Figure out the paths
    script_path = os.path.realpath(__file__)
    script_dir = os.path.dirname(script_path)
    app_dir = os.path.dirname(script_dir)
    # All commands must be performed in the app directory
    working_dir = os.getcwd()
    assert os.path.realpath(working_dir) == app_dir, 'All commands must be performed in the app directory!'
    # Default value of remote_app_dir is the same as local dir
    remote_app_dir = params['remote_app_dir'] if params['remote_app_dir'] else app_dir
    # Default value of remote_input_dir is remote_app_dir/datasets
    if params['copy_files']:
        remote_input_files_base_dir = os.path.join(remote_app_dir, 'datasets')
    if not params['internal_host_file']:
        params['internal_host_file'] = params['host_file']

    # Judge input files exist on local machine
    assert os.path.isfile(params['host_file']), 'Host file %s does not exist!' %params['host_file']
    assert os.path.isfile(params['internal_host_file']), 'Internal host file %s does not exist!' %params['internal_host_file']
    if params['copy_files']:
        assert os.path.isfile(params['word_id_file']), 'Word id file %s does not exist!' %params['word_id_file']
        assert os.path.isdir(params['binary_doc_dir']), 'Binary doc dir %s does not exist!' %params['binary_doc_dir']

    # Parse hostfile
    host_file = params['host_file']
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
        print('Error: Cannot read host file %s' %params['host_file'])
        sys.exit(1)

    # SSH options
    if params['ssh_identity_file']:
        ssh_options = ' -i %s' %params['ssh_identity_file']
    else:
        ssh_options = ''
    ssh_options += (' -oStrictHostKeyChecking=no'
        ' -oUserKnownHostsFile=/dev/null'
        ' -oLogLevel=quiet'
    )
    # SSH commands
    ssh_commands = [' '.join(['ssh', ssh_options, (params['ssh_username']+'@'+ip if params['ssh_username'] else ip)]) for ip in unique_host_list]
    scp_commands = [(' '.join(['scp', ssh_options, '-q']), (params['ssh_username']+'@'+ip if params['ssh_username'] else ip)) for ip in unique_host_list]

    # Figure out the home directory on remote servers
    ssh_cmd = ssh_commands[0]
    cmd = ' '.join([ssh_cmd, '"echo \$HOME"'])
    remote_home_dir = subprocess.check_output(cmd, shell=True).strip()
    m = re.match(r'~/(.*)', remote_app_dir)
    if m:
        remote_app_dir = os.path.join(remote_home_dir, m.group(1))
    m = re.match(r'~', remote_app_dir)
    if m:
        remote_app_dir = remote_home_dir
    m = re.match(r'~/(.*)', remote_input_files_base_dir)
    if m:
        remote_input_files_base_dir = os.path.join(remote_home_dir, m.group(1))
    m = re.match(r'~', remote_input_files_base_dir)
    if m:
        remote_input_files_base_dir = remote_home_dir
    # Create directories if not present
    print('Creating directories if not present in local')
    need_mkdirs = [params['tmp_directory']]
    for need_mkdir in need_mkdirs:
        if not os.path.exists(need_mkdir):
            os.makedirs(need_mkdir)
    print('Creating done!')
    # Create output and log directory if not present
    print('Creating output and log directory if not present on clients')
    replace_tilde_path(['log_dir', 'dump_file', 'dict_meta_file', 'word_id_file', 'internal_host_file', 'binary_doc_dir'], remote_home_dir)
    log_path = params['log_dir'] if os.path.isabs(params['log_dir']) else os.path.join(remote_app_dir, params['log_dir'])
    dump_dir = os.path.dirname(params['dump_file'])
    dump_path = dump_dir if os.path.isabs(dump_dir) else os.path.join(remote_app_dir, dump_dir)
    dict_meta_dir = os.path.dirname(params['dict_meta_file'])
    dict_meta_path = dict_meta_dir if os.path.isabs(dict_meta_dir) else os.path.join(remote_app_dir, dict_meta_dir)
    need_mkdirs = [log_path, dump_path, dict_meta_path]
    if params['copy_files']:
        need_mkdirs.append(remote_input_files_base_dir)
        need_mkdirs.append(os.path.join(remote_input_files_base_dir, os.path.basename(params['binary_doc_dir'])))
    cmds = [' '.join([ssh_cmd, '"test -d %s || mkdir -p %s"' %(need_mkdir, need_mkdir)]) for ssh_cmd in ssh_commands for need_mkdir in need_mkdirs]
    multiprocess_exec_cmds(set(cmds))
    print('Creating done!')

    # Copy necessary files
    if params['copy_files']:
        print('Copying necessary files')
        files_to_copy = [params['word_id_file'], params['internal_host_file']]
        cmds = [' '.join([scp_cmd[0], file_to_copy, scp_cmd[1]+':'+remote_input_files_base_dir]) for scp_cmd in scp_commands for file_to_copy in files_to_copy]
        multiprocess_exec_cmds(set(cmds))
        block_file_list = glob.glob(os.path.join(params['binary_doc_dir'], 'block*'))
        block_id_list = [int(re.match(r'.*block\.(\d+)', block_filename).group(1)) for block_filename in block_file_list]
        num_blocks = len(block_id_list)
        assert num_blocks > 0, 'No blocks in %s found!' %params['binary_doc_dir']
        block_offset = params['block_offset']
        assert block_offset == min(block_id_list), 'Min of block id must be the same as block_offset'
        ave_block_num = num_blocks / num_hosts
        client_block_offset = block_offset
        cmds = []
        client_id = 0
        for scp_cmd in scp_commands:
            if client_block_offset + ave_block_num > block_offset + num_blocks:
                client_num_blocks = num_blocks - client_block_offset
            else:
                if client_id == num_hosts - 1:
                    client_num_blocks = num_blocks - client_block_offset
                else:
                    client_num_blocks = ave_block_num
            for num in range(client_num_blocks):
                cmd = ' '.join([scp_cmd[0], os.path.join(params['binary_doc_dir'], 'block.%d' %(client_block_offset+num)), scp_cmd[1]+':'+os.path.join(remote_input_files_base_dir, os.path.basename(params['binary_doc_dir']))])
                cmds.append(cmd)
                cmd = ' '.join([scp_cmd[0], os.path.join(params['binary_doc_dir'], 'vocab.%d' %(client_block_offset+num)), scp_cmd[1]+':'+os.path.join(remote_input_files_base_dir, os.path.basename(params['binary_doc_dir']))])
                cmds.append(cmd)
            client_block_offset += client_num_blocks
            client_id += 1
        multiprocess_exec_cmds(set(cmds))
        print('Copying done!')

    # Count vocab size
    ssh_cmd = ssh_commands[0]
    if params['copy_files']:
        remote_word_id_file = os.path.join(remote_input_files_base_dir, os.path.basename(params['word_id_file']))
    else:
        if os.path.isabs(params['word_id_file']):
            remote_word_id_file = params['word_id_file']
        else:
            remote_word_id_file = os.path.join(remote_app_dir, params['word_id_file'])
    cmd = ' '.join([ssh_cmd, '"wc -l %s"' %(remote_word_id_file)])
    try:
        vocab_size = int(subprocess.check_output(cmd, shell=True).split()[0].strip())
    except:
        print >> sys.stderr, 'Cannot get vocab size!'
        sys.exit(1)
    # Set parameters
    if params['copy_files']:
        host_file = os.path.join(remote_input_files_base_dir, os.path.basename(params['internal_host_file']))
    else:
        if os.path.isabs(params['internal_host_file']):
            host_file = params['internal_host_file']
        else:
            host_file = os.path.join(remote_app_dir, params['internal_host_file'])
    if params['copy_files']:
        doc_file = os.path.join(remote_input_files_base_dir, os.path.basename(params['binary_doc_dir']), 'block')
        vocab_file = os.path.join(remote_input_files_base_dir, os.path.basename(params['binary_doc_dir']), 'vocab')
    else:
        if os.path.isabs(params['binary_doc_dir']):
            doc_file = os.path.join(params['binary_doc_dir'], 'block')
            vocab_file = os.path.join(params['binary_doc_dir'], 'vocab')
        else:
            doc_file = os.path.join(remote_app_dir, params['binary_doc_dir'], 'block')
            vocab_file = os.path.join(remote_app_dir, params['binary_doc_dir'], 'vocab')
    params['num_clients'] = num_hosts
    params['num_vocabs'] = vocab_size
    params['hostfile'] = host_file
    params['word_id_file'] = remote_word_id_file
    params['doc_file'] = doc_file
    params['vocab_file'] = vocab_file
    params['dump_file'] = os.path.join(dump_path, 'dump')
    params['dict_meta_file'] = os.path.join(dict_meta_path, os.path.basename(params['dict_meta_file']))

    # Generate vocab meta info
    print('Generating vocab meta info for %d hosts' %num_hosts)
    progname = 'dump_dict_meta_mn'
    prog_path = os.path.join(remote_app_dir, 'bin', progname)
    client_id = 0
    cmds = []
    for ssh_cmd in ssh_commands:
        cmd = ssh_cmd + ' '
        cmd += '"'
        cmd += 'GLOG_logtostderr=False '
        cmd += 'GLOG_log_dir=%s ' %log_path
        cmd += 'GLOG_v=-1 '
        cmd += 'GLOG_minloglevel=0 '
        cmd += 'GLOG_vmodule= '
        cmd += prog_path
        cmd += ' --undefok=' + ','.join('%s' %k for k in params) + ' '
        cmd += ''.join([' --%s=%s' % (k,v) for k,v in params.items()])
        cmd += '"'
        if client_id == 0:
            print(cmd)
        cmds.append(cmd)
        if params['shared_file_system']:
            break
        client_id += 1
    multiprocess_exec_cmds(cmds)
    print('Vocab meta info generated')
    progname = 'lda_main'
    prog_path = os.path.join(remote_app_dir, 'bin', progname)

    # Kill previous instances of this program
    print('Killing previous instances of %s on servers, please wait...' % progname)
    cmds = []
    for ssh_cmd in ssh_commands:
        cmd = ' '.join([ssh_cmd, 'killall -q %s' %progname])
        cmds.append(cmd)
    multiprocess_exec_cmds(cmds)
    print('All killed!')

    # Spawn program instances
    print('Spawning program instances')
    client_id = 0
    block_offset = params['block_offset']
    client_block_offset = block_offset
    if params['shared_file_system']:
        ssh_cmd = ssh_commands[0]
        cmd = ' '.join([ssh_cmd, 'ls -l %s*' %(params['doc_file'])])
        try:
            block_file_list_raw = subprocess.check_output(cmd, shell=True).split('\n')
            block_file_list = [block_filename for block_filename in block_file_list_raw if len(block_filename.strip()) > 0]
            block_id_list = [int(re.match(r'.*block\.(\d+)', block_filename).group(1)) for block_filename in block_file_list]
            assert block_offset == min(block_id_list), 'Min of block id must be the same as block_offset'
            num_blocks = len(block_id_list)
            ave_block_num = num_blocks / num_hosts
        except:
            print >> sys.stderr, 'Cannot get number of blocks on host %s' %ip
            sys.exit(1)
    else:
        num_blocks_by_ip = {}
        block_offset_by_ip = {}
        for ssh_cmd in ssh_commands:
            ip = re.search(r'(\d+\.\d+\.\d+\.\d+)', ssh_cmd).group(1)
            cmd = ' '.join([ssh_cmd, 'ls -l %s*' %params['doc_file']])
            try:
                block_file_list_raw = subprocess.check_output(cmd, shell=True).split('\n')
                block_file_list = [block_filename for block_filename in block_file_list_raw if len(block_filename.strip()) > 0]

                block_id_list = [int(re.match(r'.*block\.(\d+)', block_filename).group(1)) for block_filename in block_file_list]
                block_offset_by_ip[ip] = min(block_id_list)
                num_blocks_by_ip[ip] = len(block_id_list)
            except:
                print >> sys.stderr, 'Cannot get number of blocks on host %s' %ip
                sys.exit(1)
    for ssh_cmd in ssh_commands:
        ip = re.search(r'(\d+\.\d+\.\d+\.\d+)', ssh_cmd).group(1)
        print('Running client %d on %s' %(client_id, ip))
        # Set client specific parameters
        params['meta_name'] = '%s.%d' %(params['dict_meta_file'], client_id)
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

        cmd = ssh_cmd + ' '
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
        if client_id == 0:
          print(cmd)   # print the cmd for just the first machine.
          print 'Waiting for name node to set up...'
          time.sleep(3)
        client_id += 1
    print('All instances set up and running now!')
    # Sync params in other files:
    if params['sync_params']:
        # Judge scripts not in temp directory
        assert not script_path.startswith(os.path.realpath(params['tmp_directory'])
            ), 'Temp files directory must not contain scripts file (as temp files directory will be deleted)'
        # Sync kill_lda.py 
        kill_lda_path = os.path.join(script_dir, 'kill_lda.py')
        tmp_kill_lda_path = os.path.join(params['tmp_directory'], 'kill_lda.py')
        assert os.path.isfile(kill_lda_path), 'Sync failed, kill_lda.py not found!'
        with codecs.open(kill_lda_path, encoding='utf-8', mode='r') as fin:
            with codecs.open(tmp_kill_lda_path, encoding='utf-8', mode='w') as ftmp:
                curr_timestamp = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
                for line in fin:
                    line_out = re.sub(ur"'host_file'\s*:.*", ur"'host_file': '%s' # Synced at %s" %(params['host_file'], curr_timestamp), line)
                    line_out = re.sub(ur"'ssh_identity_file'\s*:.*", ur"'ssh_identity_file': '%s' # Synced at %s" %(params['ssh_identity_file'], curr_timestamp), line_out)
                    line_out = re.sub(ur"'ssh_username'\s*:.*", ur"'ssh_username': '%s' # Synced at %s" %(params['ssh_username'], curr_timestamp), line_out)
                    ftmp.write(line_out)
        if os.path.isfile(tmp_kill_lda_path):
            os.rename(tmp_kill_lda_path, kill_lda_path)
        # Sync query_status.py 
        query_status_path = os.path.join(script_dir, 'query_status.py')
        tmp_query_status_path = os.path.join(params['tmp_directory'], 'query_status.py')
        assert os.path.isfile(query_status_path), 'Sync failed, query_status.py not found!'
        with codecs.open(query_status_path, encoding='utf-8', mode='r') as fin:
            with codecs.open(tmp_query_status_path, encoding='utf-8', mode='w') as ftmp:
                curr_timestamp = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
                for line in fin:
                    line_out = re.sub(ur"'host_file'\s*:.*", ur"'host_file': '%s' # Synced at %s" %(params['host_file'], curr_timestamp), line)
                    line_out = re.sub(ur"'ssh_identity_file'\s*:.*", ur"'ssh_identity_file': '%s' # Synced at %s" %(params['ssh_identity_file'], curr_timestamp), line_out)
                    line_out = re.sub(ur"'ssh_username'\s*:.*", ur"'ssh_username': '%s' # Synced at %s" %(params['ssh_username'], curr_timestamp), line_out)
                    ftmp.write(line_out)
        if os.path.isfile(tmp_query_status_path):
            os.rename(tmp_query_status_path, query_status_path)
        # Sync download_data.py 
        download_data_path = os.path.join(script_dir, 'gce/download_data.py')
        tmp_download_data_path = os.path.join(params['tmp_directory'], 'download_data.py')
        assert os.path.isfile(download_data_path), 'Sync failed, download_data.py not found!'
        with codecs.open(download_data_path, encoding='utf-8', mode='r') as fin:
            with codecs.open(tmp_download_data_path, encoding='utf-8', mode='w') as ftmp:
                curr_timestamp = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
                for line in fin:
                    line_out = re.sub(ur"'host_file'\s*:.*", ur"'host_file': '%s' # Synced at %s" %(params['host_file'], curr_timestamp), line)
                    line_out = re.sub(ur"'ssh_identity_file'\s*:.*", ur"'ssh_identity_file': '%s' # Synced at %s" %(params['ssh_identity_file'], curr_timestamp), line_out)
                    line_out = re.sub(ur"'ssh_username'\s*:.*", ur"'ssh_username': '%s' # Synced at %s" %(params['ssh_username'], curr_timestamp), line_out)
                    line_out = re.sub(ur"'log_dir'\s*:.*", ur"'log_dir': '%s' # Synced at %s" %(log_path, curr_timestamp), line_out)
                    line_out = re.sub(ur"'dump_dir'\s*:.*", ur"'dump_dir': '%s' # Synced at %s" %(dump_path, curr_timestamp), line_out)
                    ftmp.write(line_out)
        if os.path.isfile(tmp_download_data_path):
            os.rename(tmp_download_data_path, download_data_path)
        print('Parameters synced at %s' %curr_timestamp)
    # Delete temp directory
    shutil.rmtree(params['tmp_directory'])

