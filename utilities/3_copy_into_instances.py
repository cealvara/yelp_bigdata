import os
import subprocess
import sys

from helper_get_info_instances import get_info_instances

def copy_files(filelist):
    '''
    Function to copy a given file list into instances
    '''
    for filename in filelist:
        assert os.path.exists(filename), 'Invalid filename'

        files += ' ' + filename

    for instance in instances_list:
        iname = instance['NAME']
    
        subprocess.call('gcloud compute copy-files {f} {n}:~/'.format(f=files,n=iname), shell=True)

if __name__ == '__main__':
    
    instances_list = get_info_instances()

    files = sys.argv[1:]

    copy_files(files)