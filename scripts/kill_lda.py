#!/usr/bin/env python
# Parameters
params = {
    'host_filename': 'scripts/localserver'
}

###############################################################################
# Spawn Petuum Sparse Coding on hosts listed in hostfile
import sys, os, subprocess, time, glob, codecs, re

if __name__ == '__main__':
    # Figure out the paths.
    script_path = os.path.realpath(__file__)
    script_dir = os.path.dirname(script_path)
    app_dir = os.path.dirname(script_dir)

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

    progname = 'lda_main'
    prog_path = os.path.join(app_dir, 'bin', progname)
    # SSH options
    ssh_options = (' -oStrictHostKeyChecking=no'
        ' -oUserKnownHostsFile=/dev/null'
        ' -oLogLevel=quiet'
    )

    # Kill previous instances of this program
    print('Killing previous instances of %s on servers, please wait...' % progname)
    for ip in unique_host_list:
        cmd = ' '.join(['ssh', ssh_options, ip, 'killall -q %s' %progname])
        os.system(cmd)
    print('All killed!')

