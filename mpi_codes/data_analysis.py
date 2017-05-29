import pandas as pd
import pickle

from sklearn.linear_model import LinearRegression 

if __name__ == '__main__':
    
    #read data
    with open('data_storage.pkl', 'rb') as f:
        data_storage = pickle.load(f)

    data = []
    for chunk in data_storage:
        for row in chunk:
            data.append(row)

    df = pd.DataFrame(data)

    X = df[[1, 2]]
    y = df[[0]]

    model = LinearRegression()

    model.fit(X, y)

    print(dir(model))
    try:
        print(model.coef_)
    except:
        pass
    print(df.columns)
    print(df.head())
