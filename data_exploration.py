import json
import os
import sqlite3


PATH = './yelp_dataset_challenge_round9'
#DATASETS = ['business', 'checkin', 'review', 'tip', 'user']
BUSINESS_DATASET = 'yelp_academic_dataset_business'
REVIEW_DATASET = 'yelp_academic_dataset_review'

def create_tables(conn):
    c = conn.cursor()

    #c.execute('''DROP TABLE IF EXISTS BUSINESS''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS BUSINESS (
            business_id TEXT PRIMARY KEY,
            name TEXT,
            state TEXT,
            city TEXT,
            postal_code TEXT,
            stars TEXT,
            review_count TEXT,
            longitude TEXT,
            latitude TEXT);
        ''')

    c.execute('''DROP TABLE IF EXISTS REVIEW''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS REVIEW (
            'review_id' TEXT PRIMARY KEY, 
            'business_id' TEXT, 
            'user_id' TEXT,
            'text' TEXT,
            'useful' TEXT, 
            'type' TEXT, 
            'stars' TEXT, 
            'date' TEXT);
         ''')

    conn.commit()

def business_to_sql(conn, redo=True):
    if redo:    
        columns = ['business_id', 'name', 'state', 'city', 
            'postal_code', 'stars', 'review_count', 'longitude', 'latitude']
       
        filepath = os.path.join(PATH, BUSINESS_DATASET + '.json')

        sqlcursor = conn.cursor()

        with open(filepath, 'r') as f:
            for line in f:
                jsondata = json.loads(line)
                dataload = []
                for c in columns:
                    dataload.append(jsondata[c])

                sqlcursor.execute('''INSERT INTO BUSINESS VALUES (?,?,?,?,?,?,?,?,?)''', dataload)

                conn.commit()


def review_to_sql(conn, redo=True):

    filepath = os.path.join(PATH, REVIEW_DATASET + '.json')

    sqlcursor = conn.cursor()
    columns = ['review_id', 'business_id', 'user_id', 'text', 
        'useful', 'type', 'stars', 'date']

    with open(filepath, 'r') as f:
        for line in f:
            jsondata = json.loads(line)
            dataload = []
            for c in columns:
                dataload.append(jsondata[c])

            sqlcursor.execute('''INSERT INTO REVIEW VALUES (?,?,?,?,?,?,?,?)''', dataload)
        
            conn.commit()

def main():
    
    conn = sqlite3.connect('example.db')

    create_tables(conn)

    business_to_sql(conn, False)
    
    review_to_sql(conn, True)

if __name__ == '__main__':
    main()