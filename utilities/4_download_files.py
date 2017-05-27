import os
import subprocess
import threading
from helper_get_info_instances import get_info_instances

def get_info_total_files():

    subprocess.call('gsutil ls -l gs://data-cs123/products_by_category > info_files.txt', shell=True)
    with open('info_files.txt', 'r') as f:
        info_files = []
        for line in f:
            data = line.split()
            if len(data) != 3:
                continue

            filesize = int(data[0])

            if filesize > 0:
                filename = data[2]
                info_files.append( (filesize, filename))

    for info in info_files:
        print(info)

    subprocess.call('sudo rm info_files.txt', shell=True)
    return info_files

def get_splits(info_files, k):
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

class myThread (threading.Thread):
    def __init__(self, threadID, list_of_files):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.files = list_of_files
    def run(self):
        for filename in list_of_files:
            subcommand = 'gsutil cp {} ~/'.format(filename)
            command = "gcloud compute ssh {}".format(instance_name) + \
                " --command=' {}'".format(subcommand)
            subprocess.call(command, shell=True)


def download_files_into_vms(list_of_splits, instances):

    threads = []
    
    for i, (totalsize, list_of_files) in enumerate(list_of_splits):
        instance_name = instances[i]['NAME']

        thread = myThread(i,list_of_files)
        threads.append(thread)

        # for filename in list_of_files:
        #     subcommand = 'gsutil cp {} ~/'.format(filename)
        #     command = "gcloud compute ssh {}".format(instance_name) + \
        #         " --command=' {}'".format(subcommand)
        #     subprocess.call(command, shell=True)

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