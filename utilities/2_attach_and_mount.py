import subprocess

from 1_get_info_instances import get_info_instances

def attach(instance, disk_name):
    '''
    Function to attach a given disk name into the instances
    in read-only mode
    '''
    instance_name = instance['NAME']

    command = 'gcloud compute instances ' + \
        'attach-disk {} '.format(instance_name) + \
        '--disk={} --zone=us-central1-c '.format(disk_name) + \
        '--project=cs123project --mode=ro'

    subprocess.call(command, shell=True)

def mount(instance):
    '''
    Function to mount the attached disk into each instance
    '''
    instance_name = instance['NAME']

    subcommand = 'sudo mkdir /mnt/storage; sudo mount /dev/sdb /mnt/storage'

    command = "gcloud compute ssh {}".format(instance_name) + \
        " --command=' {}'".format(subcommand)

    subprocess.call(command, shell=True)

if __name__ == '__main__':
    disk_name = 'ssd-disk'

    instances = get_info_instances()

    for instance in instances:

        attach(instance, disk_name)

        mount(instance)