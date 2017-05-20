import os
import sys

from get_info_instances import get_info_instances

if __name__ == '__main__':
    
    instances = get_info_instances()

    files = ''
    for filename in sys.argv[1:]:
        assert os.path.exists(filename), 'Invalid filename'

        files += ' ' + filename

    for instance in instances_list:
        iname = instance['NAME']
    
        subprocess.call('gcloud compute copy-files {f} {n}:~/'.format(f=files,n=iname), shell=True)
