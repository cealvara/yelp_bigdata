import subprocess

def get_info_instances():
    '''
    Function to get running instances information.

    It queries GS to get all instances, and then extract relevant information from
    them, such as external_ip, name, internal_ip, etc.

    Furthermore, this function selects only instances related to MPI,
    that is, instances that were created by the script "1_start_instances.py"

    '''
    query = 'gcloud compute instances list > instances_out.txt'

    subprocess.call(query, shell=True)

    f =  open('instances_out.txt', 'r')

    header = f.readline().strip()
    
    if len(header) < 2:
        print('No instances open')
        f.close()
        return None

    # Storing the "header" names into a list
    info_pos = []
    for column_name in header.split():
        info_pos.append((column_name, header.find(column_name)))

    # Iterate over rows to get each instance's information
    instances = []
    for line in f:
        instance = {}
        for i, (column_name, ini_pos) in enumerate(info_pos):
            try:
                end_pos = info_pos[i+1][1] - 1
            except:
                end_pos = ini_pos + 20

            instance[column_name] = line[ini_pos:end_pos].strip()
        
        if 'mpi-instance' in instance['NAME']:
            instances.append(instance)

    # Print instances info to get useful data (to SSH, for example)
    print(instances)

    f.close()
    
    if instances:
        return instances

    return None

if __name__ == '__main__':
    get_info_instances()