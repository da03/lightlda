#!/usr/bin/env python
# Parameters
params = {
    'download_dir': 'download'
    # Default parameters, shall be synced after run run_lda.py:
    , 'host_file': '/home/yuntiand/daiwei/wrapup_preprocess/update7/lightlda/machinefiles/gce_hosts' # Synced at Mon, 27 Apr 2015 21:28:42 GMT
    , 'remote_app_dir': '~/lightlda' # Synced at Tue, 28 Apr 2015 00:18:32 GMT
    , 'log_dir': 'log'
    , 'dump_dir': 'output'
    , 'ssh_identity_file': '~/.ssh/google_compute_engine' # Synced at Mon, 27 Apr 2015 21:28:42 GMT
    , 'ssh_username': 'lightlda' # Synced at Mon, 27 Apr 2015 21:28:42 GMT
}

###############################################################################
import sys, os, subprocess, time, glob, codecs, re, multiprocessing

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
    # Parse hostfile
    host_file = os.path.realpath(params['host_file'])
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
    remote_app_dir = params['remote_app_dir']
    assert remote_app_dir, 'remote_app_dir must be set!'
    m = re.match(r'~/(.*)', remote_app_dir)
    if m:
        remote_app_dir = os.path.join(remote_home_dir, m.group(1))
    m = re.match(r'~', remote_app_dir)
    if m:
        remote_app_dir = remote_home_dir
    replace_tilde_path(['log_dir', 'dump_dir'], remote_home_dir)
    # Figure out paths
    if os.path.isabs(params['log_dir']):
        log_path = params['log_dir']
    else:
        log_path = os.path.join(remote_app_dir, params['log_dir'])
    if os.path.isabs(params['dump_dir']):
        dump_path = params['dump_dir']
    else:
        dump_path = os.path.join(remote_app_dir, params['dump_dir'])
    local_log_path = os.path.join(params['download_dir'], os.path.basename(params['log_dir']))
    local_dump_path = os.path.join(params['download_dir'], os.path.basename(params['dump_dir']))

    # Create directories if not present
    print('Creating directories if not present in local')
    need_mkdirs = [local_log_path, local_dump_path]
    for need_mkdir in need_mkdirs:
        if not os.path.exists(need_mkdir):
            os.makedirs(need_mkdir)
    print('Creating done!')

    # Download data
    print('Downloading...')
    cmds = []
    for scp_cmd in scp_commands:
        cmd = ' '.join([scp_cmd[0], scp_cmd[1]+':'+os.path.join(log_path,'*'), local_log_path])
        cmds.append(cmd)
        cmd = ' '.join([scp_cmd[0], scp_cmd[1]+':'+os.path.join(dump_path,'*'), local_dump_path])
        cmds.append(cmd)
    multiprocess_exec_cmds(cmds)
    print('Downloaded!')

