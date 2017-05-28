import os
import subprocess
import threading
from helper_get_info_instances import get_info_instances

GSPATH = 'gs://data-cs123/products_by_category/'

class myThread(threading.Thread):
    '''
    Thread object to download files from multiple VMs at the same time
    (not having to wait for one VM to finish to start the other)
    '''
    def __init__(self, threadID, list_of_files, instance_name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.files = list_of_files
        self.instance_name = instance_name
    def run(self):
        for filename in self.files:
            subcommand = 'sudo gsutil cp {p}{a} /mnt/local/data/{a}'.format(p=GSPATH, a=filename)
            command = "gcloud compute ssh {}".format(self.instance_name) + \
                " --command=' {}'".format(subcommand)
            subprocess.call(command, shell=True)

def get_info_total_files():
    '''
    Preliminary function to get the list of files to be copied to the VMs

    Returns: a list of files to be copied to the VMs, along with their size
    (so we can split them by size across the VMs)
    '''
    subprocess.call('gsutil ls -l {} > info_files.txt'.format(GSPATH), shell=True)
    with open('info_files.txt', 'r') as f:
        info_files = []
        for line in f:
            data = line.split()
            if len(data) != 3:
                continue

            filesize = int(data[0])

            if filesize > 0:
                filepath = data[2]
                filename = filepath[len(GSPATH):]
                info_files.append( (filesize, filename))

    for info in info_files:
        print(info)

    subprocess.call('sudo rm info_files.txt', shell=True)
    return info_files

def get_splits(info_files, k):
    '''
    Function to split a list of (filesize, filename)
    into K buckets (for K VMs)

    Returns: a list with K lists, one for each VM
    '''
    l = sorted(info_files, reverse=True)
    nodes = list(range(k))
    for node in range(k):
        nodes[node] = [l[node][0],[l[node][1]]]

    for remaining in l[k:]:
        nodes = sorted(nodes)
        smallest = nodes[0]
        smallest[0] += remaining[0]
        smallest[1].append(remaining[1])
        nodes[0] = smallest

    return nodes



def download_files_into_vms(list_of_splits, instances):
    '''
    Main Function. It downloads files from GS to each instances

    It uses multithreading, to avoid waiting between downloads.
    '''
    threads = []
    
    for i, (totalsize, list_of_files) in enumerate(list_of_splits):
        instance_name = instances[i]['NAME']

        thread = myThread(i, list_of_files, instance_name)
        threads.append(thread)

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

if __name__ == '__main__':
    instances = get_info_instances()

    try:
        n_instances = len(instances)
    except:
        n_instances = 3

    info_files = get_info_total_files()

    splits = get_splits(info_files, n_instances)
    
    print(splits)

    download_files_into_vms(splits, instances)

    print('Finished downloading files')