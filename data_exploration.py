import os
import itertools

PATH = './yelp_dataset_challenge_round9'
#DATASETS = ['business', 'checkin', 'review', 'tip', 'user']
DATASET = 'yelp_academic_dataset_business'

def main():
    file1 = os.path.join(PATH, DATASET + '.json')
    file2 = os.path.join(PATH, DATASET + '2.json')

    count = 0
    for line1, line2 in itertools.product(open(file1), open(file2)):
        count += 1
    
    print(count)

if __name__ == '__main__':
    main()