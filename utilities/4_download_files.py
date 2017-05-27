import os
import subprocess

from 1_get_info_instances import get_info_instances

def get_info_total_files():

    subprocess.call('gsutil ls -l gs://data-cs123/products_by_category > info_files.txt', shell=True)
    with open('info_files.txt', 'r') as f:
        info_files = []
        for line in f:
            data = line.split()
            print(len(data))

            if len(data) == 3:
                filesize = data[0]
                filename = data[2]
                info_files.append( (filename, filesize))

    for info in info_files:
        print(info)

    subprocess.call('sudo rm info_files.txt', shell=True)
    return info_files

def get_splits(info_files, k):
    pass
    
def download_files_into_vms():
    pass


if __name__ == '__main__':
    instances = get_info_instances()

    n_instances = len(instances)

    info_files = get_info_total_files()

    splits = get_splits(info_files, n_instances)
    
    download_files_into_vms()