import subprocess




def main():
    instances = get_info_instances()
    if instances:

        instance_names = ' '.join([data['NAME'] for data in instances])

        kill_instances = 'gcloud compute instances delete {} --zone=us-central1-c'.format(instance_names)
        print(kill_instances)
        subprocess.call(kill_instances, shell=True)

if __name__ == '__main__':
    main()