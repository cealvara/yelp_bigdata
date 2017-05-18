import subprocess
import sys
import time

from get_info_instances import get_info_instances

def create_instances(nmachines):

    instance_list = ' '.join(['instance-{}'.format(i) for i in range(nmachines)])
    
    query = 'gcloud compute instances create ' + instance_list + \
            ' --zone=us-central1-c' + \
            ' --metadata-from-file=startup-script=base_script.sh'

    print(query)
    subprocess.call(query, shell=True)

def create_hosts_file(instances_list):
    subprocess.call('rm hosts', shell=True)
    for instance in instances_list:
        subprocess.call('echo {} >> hosts'.format(instance['INTERNAL_IP']), shell=True)


def copy_files(instances_list):
    for instance in instances_list:
        ext_ip = instance['EXTERNAL_IP']
        subprocess.call('scp -i ~/.ssh/google-cloud-cs123 -o StrictHostKeyChecking=no ~/.ssh/google-cloud-cs123 {}:~/.ssh/id_rsa'.format(ext_ip), shell=True)
        subprocess.call('scp -i ~/.ssh/google-cloud-cs123 hosts {}:~/hosts'.format(ext_ip), shell=True)


if __name__ == '__main__':
    try:
        N_MACHINES = int(sys.argv[1])
    except:
        N_MACHINES = 2

    create_instances(N_MACHINES)

    time.sleep(5)

    instances = get_info_instances()

    create_hosts_file(instances)

    time.sleep(5)

    copy_files(instances)