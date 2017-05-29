import subprocess
import sys
import time

from helper_get_info_instances import get_info_instances

def create_instances(nmachines):
    '''
    Function that creates N machines on google cloud

    Input: number of machines
    Returns: Nothing (creates machines on google cloud)
    '''
    instance_list = ' '.join(
        ['mpi-instance-{}'.format(i) for i in range(nmachines)])
    
    query = 'gcloud compute instances create ' + instance_list + \
            ' --zone=us-central1-c' + \
            ' --metadata-from-file=startup-script=base_script.sh' + \
            ' --boot-disk-size=50GB'

    print(query)
    subprocess.call(query, shell=True)


if __name__ == '__main__':
    try:
        N_MACHINES = int(sys.argv[1])
    except:
        N_MACHINES = 2

    create_instances(N_MACHINES)