import json
import os
import sqlite3


PATH = './yelp_dataset_challenge_round9'
#DATASETS = ['business', 'checkin', 'review', 'tip', 'user']

#THESE COLUMNS NEED TO BE IN THE SAME ORDER AS THE CORRESPONDING SQL TABLES

BUSINESS_DATASET = {
    'filename' : 'yelp_academic_dataset_business.json',
    'columns' : ['business_id', 'name', 'state', 'city', 
        'postal_code', 'stars', 'review_count', 'longitude', 'latitude'], 
    'tablename' : 'BUSINESS'}

REVIEW_DATASET = {
    'filename' : 'yelp_academic_dataset_review.json', 
    'columns' : ['review_id', 'business_id', 'user_id', 'text', 
        'useful', 'type', 'stars', 'date'], 
    'tablename': 'REVIEW'}

DATASETS = [BUSINESS_DATASET, REVIEW_DATASET]


def create_tables(conn, redo=False):
    c = conn.cursor()

    if redo:
        c.execute('''DROP TABLE IF EXISTS BUSINESS''')
        c.execute('''DROP TABLE IF EXISTS REVIEW''')

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

def json_to_sql(conn, datainfo, redo=False, skip=0):
    if redo:
        filename = datainfo['filename']
        columns = datainfo['columns']
        tablename = datainfo['tablename']
        questionmarks = "(" + ",".join(["?" for _ in range(len(columns))]) + ")"

        filepath = os.path.join(PATH, filename)

        sqlcursor = conn.cursor()

        with open(filepath, 'r') as f:
            for _ in range(0,skip):
                next(f)

            for line in f:
                jsondata = json.loads(line)
                dataload = []
                for c in columns:
                    dataload.append(jsondata[c])

                try:
                    print('here')
                    sqlcursor.execute('''
                        INSERT INTO {} 
                        VALUES (?,?,?,?,?,?,?,?,?)
                        '''.format(tablename, questionmarks), dataload)
                    conn.commit()
                except:
                    pass

def main():
    
    conn = sqlite3.connect('example.db')

    create_tables(conn)

    for filename, columns in REVIEW_DATASET.items():
        json_to_sql(conn, filename, columns, True, 4013070)
    

if __name__ == '__main__':
    main()