#!/usr/bin/env python
# Create multiple gce instances
# Parameters
params = {
    'instance_name_prefix': 'lightlda'
    , 'num_instances': 2
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

def create_instance(instance_name):
    print('Creating %s' %instance_name)
    subprocess.call('gcloud compute instances create %s --image %s --zone %s --machine-type %s -q' %(
        instance_name, params['image'], params['zone'], params['machine_type']), shell=True)
    print('Instance %s created' %instance_name)

if __name__ == '__main__':
    # Figure out the paths
    script_path = os.path.realpath(__file__)
    script_dir = os.path.dirname(script_path)

    # Create directories if not present
    print('Creating directories if not present')
    need_mkdirs = [params['tmp_directory']]
    for need_mkdir in need_mkdirs:
        if not os.path.exists(need_mkdir):
            os.makedirs(need_mkdir)
    print('Creating done!')

    threads = []
    instance_id_offset = params['instance_id_offset']
    num_instances = params['num_instances']
    for instance_id in range(instance_id_offset, instance_id_offset+num_instances):
        instance_name = '%s-%d' %(params['instance_name_prefix'], instance_id)
        thread = multiprocessing.Process(target=create_instance, args=(instance_name, ))
        threads.append(thread)
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

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
        print('Parameters synced at %s' %curr_timestamp)
    # Delete temp directory
    shutil.rmtree(params['tmp_directory'])

    print('All created!')

