#!/usr/bin/env python
# Delete multiple gce instances
# Parameters
params = {
    'instance_name_prefix': 'lightlda' # Synced at Tue, 28 Apr 2015 00:18:32 GMT
    , 'num_instances': 2 # Synced at Tue, 28 Apr 2015 00:18:32 GMT
    # Default parameters:
    , 'instance_id_offset': 0 # Synced at Tue, 28 Apr 2015 00:18:32 GMT
    , 'zone': 'us-central1-a' # Synced at Tue, 28 Apr 2015 00:18:32 GMT
}

###############################################################################
import subprocess, os, codecs, time, multiprocessing, re, shutil

def delete_instance(instance_name):
    print('Deleting %s' %instance_name)
    subprocess.call('gcloud compute instances delete %s --zone %s -q' %(
        instance_name, params['zone']), shell=True)
    print('Instance %s deleted' %instance_name)

if __name__ == '__main__':
    threads = []
    instance_id_offset = params['instance_id_offset']
    num_instances = params['num_instances']
    for instance_id in range(instance_id_offset, instance_id_offset+num_instances):
        instance_name = '%s-%d' %(params['instance_name_prefix'], instance_id)
        thread = multiprocessing.Process(target=delete_instance, args=(instance_name, ))
        threads.append(thread)
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    print('All deleted!')


