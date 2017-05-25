import subprocess

from 1_get_info_instances import get_info_instances

def end_instances():
    '''
    Function to end all running instances (created for MPI purposes)
    '''
    subprocess.call('rm hosts', shell=True)
    instances = get_info_instances()
    if instances:

        instance_names = ' '.join([data['NAME'] for data in instances])

        kill_instances = 'gcloud compute instances delete {} --zone=us-central1-c'.format(instance_names)
        print(kill_instances)
        subprocess.call(kill_instances, shell=True)

if __name__ == '__main__':
    end_instances()