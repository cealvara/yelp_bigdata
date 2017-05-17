def get_info_instances():
    query = 'gcloud compute instances list > instances_out.txt'

    subprocess.call(query, shell=True)

    f =  open('instances_out.txt', 'r')

    header = f.readline().strip()
    
    if len(header) < 2:
        print('No instances open')
        f.close()
        return None

    info_pos = []
    for column_name in header.split():
        info_pos.append((column_name, header.find(column_name)))

    instances = []
    for line in f:
        instance = {}
        for i, (column_name, ini_pos) in enumerate(info_pos):
            try:
                end_pos = info_pos[i+1][1] - 1
            except:
                end_pos = ini_pos + 20

            instance[column_name] = line[ini_pos:end_pos].strip()
        
        #if instance['NAME'] != 'base-instance-free':
        instances.append(instance)

    print(instances)
    f.close()
    
    return instances

if __name__ == '__main__':
    get_info_instances()