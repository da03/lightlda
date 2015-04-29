#!/usr/bin/env python
# Parameters
params = {
    'host_file': '/home/yuntiand/daiwei/wrapup_preprocess/update5/lightlda/machinefiles/gce_hosts' # Synced at Mon, 27 Apr 2015 21:28:42 GMT
    # Default parameters:
    , 'ssh_identity_file': '~/.ssh/google_compute_engine' # Synced at Mon, 27 Apr 2015 21:28:42 GMT
    , 'ssh_username': 'lightlda' # Synced at Mon, 27 Apr 2015 21:28:42 GMT
}

###############################################################################
import sys, os, subprocess, time, glob, codecs, re, multiprocessing

def checkoutput_cmds(cmds, result_queue):
    for cmd in cmds:
        result = subprocess.check_output(cmd, shell=True)
        result_queue.put((cmds, result.strip()))

def multiprocess_checkoutput_cmds(cmds):
    cmds_ip_dict = {}
    for cmd in cmds:
        ip = re.search(r'(\d+\.\d+\.\d+\.\d+)', cmd).group(1)
        if ip not in cmds_ip_dict:
            cmds_ip_dict[ip] = []
        cmds_ip_dict[ip].append(cmd)
    threads = []
    result_queue = multiprocessing.Queue()
    for ip in cmds_ip_dict:
        thread = multiprocessing.Process(target=checkoutput_cmds, args=(cmds_ip_dict[ip], result_queue))
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

    progname = 'lda_main'
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

    # Query the status of this program
    print('Querying the status of program')
    cmds = []
    for ssh_cmd in ssh_commands:
        cmd = ' '.join([ssh_cmd, '\'ps axf | grep %s | grep -v grep | awk "{print  $1}"\'' %progname])
        cmds.append(cmd)
    num_threads, result_queue = multiprocess_checkoutput_cmds(cmds)
    num_running = 0
    for thread_id in range(num_threads):
        try:
            cmds, result = result_queue.get(False)
            if result[1]:
                num_running += 1
        except:
            print >> sys.stderr, 'Error checkout commands'
            break
    if num_running > 0:
        print('%d instances running %s' %(num_running, progname))
    else:
        print('None of the intances running %s' %progname)

