#!/usr/bin/env python
# Create multiple gce instances
# Parameters
params = {
    'instance_name_prefix': 'lightlda'
    , 'num_instances': 2
    , 'host_file': 'machinefiles/gce_hosts'
    , 'internal_host_file': 'machinefiles/gce_internal_hosts'
    # Default parameters:
    , 'instance_id_offset': 0
    , 'image': 'lightlda'
    , 'machine_type': 'n1-standard-1'
    , 'zone': 'us-central1-a'
    # Sync params in other files:
    , 'sync_params': True
    # Tmp files directory:
    , 'tmp_directory': 'tmp'
}

###############################################################################
import subprocess, os, codecs, time, multiprocessing, re, shutil

def create_instance(instance_name, result_queue):
    print('Creating %s' %instance_name)
    result = subprocess.check_output('gcloud compute instances create %s --image %s --zone %s --machine-type %s -q' %(
        instance_name, params['image'], params['zone'], params['machine_type']), shell=True)
    result_queue.put((instance_name, result))
    print('Instance %s created' %instance_name)

if __name__ == '__main__':
    # Figure out the paths
    script_path = os.path.realpath(__file__)
    script_dir = os.path.dirname(script_path)

    # Create directories if not present
    print('Creating directories if not present')
    need_mkdirs = [params['tmp_directory'], os.path.dirname(params['host_file']), os.path.dirname(params['internal_host_file'])]
    for need_mkdir in need_mkdirs:
        if not os.path.exists(need_mkdir):
            os.makedirs(need_mkdir)
    print('Creating done!')

    threads = []
    result_queue = multiprocessing.Queue()
    instance_id_offset = params['instance_id_offset']
    num_instances = params['num_instances']
    for instance_id in range(instance_id_offset, instance_id_offset+num_instances):
        instance_name = '%s-%d' %(params['instance_name_prefix'], instance_id)
        thread = multiprocessing.Process(target=create_instance, args=(instance_name, result_queue))
        threads.append(thread)
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
   
    ip_dict = {}
    for thread_id in range(len(threads)):
        try:
            instance_name, instance_result = result_queue.get(False)
            instance_id = int(re.match(r'%s-(\d+)' %params['instance_name_prefix'], instance_name).group(1))
            internal_ip = re.search(r'(\d+\.\d+\.\d+\.\d+)\s(\d+\.\d+\.\d+\.\d+)' , instance_result).group(1)
            external_ip = re.search(r'(\d+\.\d+\.\d+\.\d+)\s(\d+\.\d+\.\d+\.\d+)' , instance_result).group(2)
            ip_dict[instance_id] = (internal_ip, external_ip)
        except:
            print >> sys.stderr, 'Error occurs in creating instances, exiting now'
            break

    with open(params['host_file'], 'w') as fhost:
        host_id = 1
        for instance_id in range(instance_id_offset, instance_id_offset+num_instances):
            assert instance_id in ip_dict, 'Instance %s-%d was not created successfully!' %(params['instance_name_prefix'], instance_id)
            if instance_id == instance_id_offset:
                fhost.write('0 %s 10000' %ip_dict[instance_id][1])
                fhost.write('\n1 %s 9999' %ip_dict[instance_id][1])
            else:
                fhost.write('\n%d %s 9999' %((1000*host_id), ip_dict[instance_id][1]))
                host_id += 1

    with open(params['internal_host_file'], 'w') as fhost:
        host_id = 1
        for instance_id in range(instance_id_offset, instance_id_offset+num_instances):
            assert instance_id in ip_dict, 'Instance %s-%d was not created successfully!' %(params['instance_name_prefix'], instance_id)
            if instance_id == instance_id_offset:
                fhost.write('0 %s 10000' %ip_dict[instance_id][0])
                fhost.write('\n1 %s 9999' %ip_dict[instance_id][0])
            else:
                fhost.write('\n%d %s 9999' %((1000*host_id), ip_dict[instance_id][0]))
                host_id += 1

    # Sync params in other files:
    if params['sync_params']:
        # Judge scripts not in temp directory
        assert not script_path.startswith(os.path.realpath(params['tmp_directory'])
            ), 'Temp files directory must not contain scripts file (as temp files directory will be deleted)'
        # Sync delete_instances.py 
        delete_instances_path = os.path.join(script_dir, 'delete_instances.py')
        tmp_delete_instances_path = os.path.join(params['tmp_directory'], 'delete_instances.py')
        assert os.path.isfile(delete_instances_path), 'Sync failed, delete_instances.py not found!'
        with codecs.open(delete_instances_path, encoding='utf-8', mode='r') as fin:
            with codecs.open(tmp_delete_instances_path, encoding='utf-8', mode='w') as ftmp:
                curr_timestamp = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
                for line in fin:
                    line_out = re.sub(ur"'instance_name_prefix'\s*:.*", ur"'instance_name_prefix': '%s' # Synced at %s" %(params['instance_name_prefix'], curr_timestamp), line)
                    line_out = re.sub(ur"'num_instances'\s*:.*", ur"'num_instances': %d # Synced at %s" %(params['num_instances'], curr_timestamp), line_out)
                    line_out = re.sub(ur"'instance_id_offset'\s*:.*", ur"'instance_id_offset': %d # Synced at %s" %(params['instance_id_offset'], curr_timestamp), line_out)
                    line_out = re.sub(ur"'zone'\s*:.*", ur"'zone': '%s' # Synced at %s" %(params['zone'], curr_timestamp), line_out)
                    ftmp.write(line_out)
        if os.path.isfile(tmp_delete_instances_path):
            os.rename(tmp_delete_instances_path, delete_instances_path)
        # Sync run_lda.py 
        run_lda_path = os.path.join(script_dir, '../run_lda.py')
        tmp_run_lda_path = os.path.join(params['tmp_directory'], 'run_lda.py')
        assert os.path.isfile(run_lda_path), 'Sync failed, run_lda.py not found!'
        with codecs.open(run_lda_path, encoding='utf-8', mode='r') as fin:
            with codecs.open(tmp_run_lda_path, encoding='utf-8', mode='w') as ftmp:
                curr_timestamp = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
                for line in fin:
                    line_out = re.sub(ur"(\s*,\s*)'host_file'\s*:.*", ur"\1'host_file': '%s' # Synced at %s" %(os.path.realpath(params['host_file']), curr_timestamp), line)
                    line_out = re.sub(ur"(\s*,\s*)'internal_host_file'\s*:.*", ur"\1'internal_host_file': '%s' # Synced at %s" %(os.path.realpath(params['internal_host_file']), curr_timestamp), line_out)
                    line_out = re.sub(ur"(\s*,\s*)'ssh_identity_file'\s*:.*", ur"\1'ssh_identity_file': '~/.ssh/google_compute_engine' # Synced at %s" %(curr_timestamp), line_out)
                    line_out = re.sub(ur"(\s*,\s*)'ssh_username'\s*:.*", ur"\1'ssh_username': 'lightlda' # Synced at %s" %(curr_timestamp), line_out)
                    line_out = re.sub(ur"(\s*,\s*)'remote_app_dir'\s*:.*", ur"\1'remote_app_dir': '~/lightlda' # Synced at %s" %(curr_timestamp), line_out)
                    ftmp.write(line_out)
        if os.path.isfile(tmp_run_lda_path):
            os.rename(tmp_run_lda_path, run_lda_path)
        print('Parameters synced at %s' %curr_timestamp)
    # Delete temp directory
    shutil.rmtree(params['tmp_directory'])

    print('All created!')

