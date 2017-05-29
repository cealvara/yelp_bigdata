import pickle

if __name__ == '__main__':
    
    #read data
    with open('data_storage.pkl', 'rb') as f:
        data_storage = pickle.load(f)

    data = []
    for chunk in data_storage:
        for row in chunk:
            data.append(row)

    print('here')
    print(len(data))