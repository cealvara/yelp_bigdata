import subprocess

from get_instances_info import get_instances_info

def attach(instance, disk_name):
    instance_name = instance['NAME']

    command = 'gcloud compute instances ' + \
        'attach-disk {} '.format(instance_name) + \
        '--disk={} --zone=us-central1-c '.format(disk_name) + \
        '--project=cs123project --mode=ro'

    subprocess(command, shell=True)

def mount(instance):
    instance_name = instance['NAME']

    subcommand = 'mount /dev/sdb /mnt/storage'

    command = "gcloud compute ssh {}".format(instance_name) + \
        " --command=' {}'".format(subcommand)

    subprocess(command, shell=True)

if __name__ == '__main__':
    disk_name = 'main-disk'

    instances = get_instances_info()

    for instance in instances:

        attach(instance, disk_name)

        mount(instance)