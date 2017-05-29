import pickle

if __name__ == '__main__':
    
    #read data
    with open('data_storage.pkl', 'rb') as f:
        data_storage = pickle.load(f)

    print('here')
    print(len(data_storage))