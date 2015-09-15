#!/usr/bin/env python
# Spawn Petuum LightLDA instances
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
def run(params):
    app_dir = params['app_dir']
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
    if 'ssh_identity_file' in params:
        ssh_options = ' -i %s' %params['ssh_identity_file']
    else:
        ssh_options = ''

    ssh_options += (' -oStrictHostKeyChecking=no'
        ' -oUserKnownHostsFile=/dev/null'
        ' -oLogLevel=quiet'
    )
    # SSH commands
    ssh_commands = [' '.join(['ssh', ssh_options, (params['ssh_username']+'@'+ip if 'SSH_username' in params else ip)]) for ip in unique_host_list]
    scp_commands = [(' '.join(['scp', ssh_options, '-q']), (params['SSH_username']+'@'+ip if 'SSH_username' in params else ip)) for ip in unique_host_list]

    # Count vocab size
    word_id_file = params['word_id_file']
    cmd = 'wc -l %s' %(word_id_file)
    try:
        vocab_size = int(subprocess.check_output(cmd, shell=True).split()[0].strip())
    except:
        print >> sys.stderr, 'Cannot get vocab size!'
        sys.exit(1)

    params_gen_meta = {}
    params_gen_meta['word_id_file'] = word_id_file
    params_gen_meta['dict_meta_file'] = params['dict_meta_file']
    params_gen_meta['load_factor'] = params['load_factor']
    params_gen_meta['num_clients'] = num_hosts
    params_gen_meta['num_topics'] = params['num_topics']
    params_gen_meta['num_vocabs'] = vocab_size

    # Generate vocab meta info
    print('Generating vocab meta info for %d hosts' %num_hosts)
    progname = 'dump_dict_meta_mn'
    prog_path = os.path.join(app_dir, 'bin', progname)
    cmd = 'GLOG_logtostderr=False '
    cmd += 'GLOG_log_dir=%s ' %params['log_dir']
    cmd += 'GLOG_v=-1 '
    cmd += 'GLOG_minloglevel=0 '
    cmd += 'GLOG_vmodule= '
    cmd += prog_path
    cmd += ''.join([' --%s=%s' % (k,v) for k,v in params_gen_meta.items()])
    print(cmd)
    subprocess.call(cmd, shell=True)
    print('Vocab meta info generated')

    progname = 'lda_main'
    prog_path = os.path.join(app_dir, 'bin', progname)
    # Kill previous instances of this program
    print('Killing previous instances of %s on servers, please wait...' % progname)
    for ssh_cmd in ssh_commands:
        cmd = ' '.join([ssh_cmd, 'killall -q %s' %progname])
        subprocess.call(cmd, shell=True)
    print('All killed!')

    # Spawn program instances
    block_files = [filename for filename in glob.glob(os.path.join(params['datablocks_dir'],'block*'))]
    block_sizes = [os.path.getsize(filename) for filename in block_files]
    num_blocks = len(block_files)
    max_block_size = max(block_sizes)
    ave_block_num = num_blocks / num_hosts

    doc_file = os.path.join(params['datablocks_dir'], 'block')
    vocab_file = os.path.join(params['datablocks_dir'], 'vocab')

    params_run = {}
    params_run['hostfile'] = params['host_file']
    params_run['num_clients'] = num_hosts
    params_run['num_worker_threads'] = params['num_worker_threads']
    params_run['num_delta_threads'] = params['num_delta_threads']
    params_run['cold_start'] = params['cold_start']
    params_run['staleness'] = params['staleness']
    params_run['num_blocks'] = num_blocks
    params_run['doc_file'] = doc_file
    params_run['vocab_file'] = vocab_file
    params_run['dump_file'] = params['dump_file']
    
    params_run['alpha'] = params['alpha']
    params_run['beta'] = params['beta']
    params_run['mh_step'] = params['mh_step']
    params_run['num_vocabs'] = vocab_size
    params_run['num_topics'] = params['num_topics']
    params_run['num_iterations'] = params['num_iterations']
    params_run['dump_model_interval'] = params['dump_model_interval']
    params_run['compute_ll_interval'] = params['compute_ll_interval']

    params_run['block_size'] = int(params['block_size']) + 1
    params_run['block_max_capacity'] = max_block_size
    params_run['alias_max_capacity'] = params['alias_max_capacity']
    params_run['delta_max_capacity'] = params['delta_max_capacity']
    params_run['model_max_capacity'] = params['model_max_capacity']
    params_run['load_factor'] = params['load_factor']

    print('Spawning program instances')
    client_id = 0
    block_offset = 0
    client_block_offset = block_offset
    for ssh_cmd in ssh_commands:
        ip = re.search(r'(\d+\.\d+\.\d+\.\d+)', ssh_cmd).group(1)
        print('Running client %d on %s' %(client_id, ip))
        # Set client specific parameters
        params_run['meta_name'] = '%s.%d' %(params['dict_meta_file'], client_id)
        params_run['client_id'] = client_id
        # Set number of blocks
        if client_block_offset + ave_block_num > block_offset + num_blocks:
            client_num_blocks = num_blocks - client_block_offset
        else:
            if client_id == num_hosts - 1:
                client_num_blocks = num_blocks - client_block_offset
            else:
                client_num_blocks = ave_block_num
        params_run['num_blocks'] = client_num_blocks
        params_run['block_offset'] = client_block_offset
        client_block_offset += client_num_blocks

        cmd = ssh_cmd + ' '
        cmd += 'GLOG_logtostderr=False '
        cmd += 'GLOG_log_dir=%s ' %params['log_dir']
        cmd += 'GLOG_v=-1 '
        cmd += 'GLOG_minloglevel=0 '
        cmd += 'GLOG_vmodule= '
        cmd += prog_path
        cmd += ''.join([' --%s=%s' % (k,v) for k,v in params_run.items()])
        cmd += '&'
        subprocess.call(cmd, shell=True)
        # Wait a few seconds for the name node (client 0) to set up
        if client_id == 0:
          print(cmd)   # print the cmd for just the first machine.
          print 'Waiting for name node to set up...'
          time.sleep(3)
        client_id += 1
    print('All instances set up and running now!')
