import json
import os
import sqlite3
import ast
import time
start_time = time.time()


def create_tables(conn, redo=False):
    c = conn.cursor()

    if redo:
        c.execute('''DROP TABLE IF EXISTS METADATA''')
        c.execute('''DROP TABLE IF EXISTS ALSOVIEWED''')
        c.execute('''DROP TABLE IF EXISTS ALSOBOUGHT''')
        c.execute('''DROP TABLE IF EXISTS BOUGHTTOGETHER''')
        c.execute('''DROP TABLE IF EXISTS BUYAFTERVIEWING''')
        c.execute('''DROP TABLE IF EXISTS SALESRANK''')
        c.execute('''DROP TABLE IF EXISTS CATEGORIES''')

    c.execute('''
            CREATE TABLE IF NOT EXISTS METADATA (
            asin TEXT PRIMARY KEY,
            description TEXT,
            title TEXT,
            price TEXT,
            brand TEXT);
        ''')
    c.execute('''
            CREATE TABLE IF NOT EXISTS ALSOVIEWED (
            asin TEXT,
            asin2 TEXT);
        ''')
    c.execute('''
            CREATE TABLE IF NOT EXISTS ALSOBOUGHT (
            asin TEXT,
            asin2 TEXT);
        ''')
    c.execute('''
            CREATE TABLE IF NOT EXISTS BOUGHTTOGETHER (
            asin TEXT,
            asin2 TEXT);
        ''')
    c.execute('''
            CREATE TABLE IF NOT EXISTS BUYAFTERVIEWING (
            asin TEXT,
            asin2 TEXT);
        ''')
    c.execute('''
            CREATE TABLE IF NOT EXISTS SALESRANK (
            asin TEXT,
            category TEXT,
            rank TEXT);
        ''')
    c.execute('''
            CREATE TABLE IF NOT EXISTS CATEGORIES (
            asin TEXT,
            categories TEXT);
        ''')

    conn.commit()


PATH = './'


def json_to_sql(conn, redo=False, skip=0):
    # print("time at beginning of json_to_sql: ", time.time() - start_time)
    if redo:
        filename = 'metadata.json'

        metadata_columns = ['asin', 'description', 'title', 'price', 'brand']

        filepath = os.path.join(PATH, filename)
        sqlcursor = conn.cursor()

        with open(filepath, 'r') as f:

            for _ in range(0,skip):
                next(f)

            
            loops = 0
            total = 0
            for line in f:
                line = ast.literal_eval(line)
                metadata_load = []

                # MAIN METADATA
                for c in metadata_columns:
                    if c in line.keys():
                        metadata_load.append(line[c])
                    else:
                        metadata_load.append("")

                try:
                    sqlcursor.execute('''
                        INSERT INTO METADATA
                        VALUES {}
                        '''.format("(?,?,?,?,?)"), metadata_load)
                except:
                    print("something went wrong with ", line)


                # SALES RANK
                if 'salesRank' in line.keys():
                    for key, value in line['salesRank'].items():
                        try:
                            sqlcursor.execute('''
                                INSERT INTO SALESRANK
                                VALUES {}
                                '''.format("(?, ?, ?)"), [line['asin'], key, value])
                        except:
                            print("something went wrong with ", line)

                # CATEGORIES
                if 'categories' in line.keys():
                    for cats in line['categories']:
                        for cat in cats:
                            try:
                                sqlcursor.execute('''
                                    INSERT INTO CATEGORIES
                                    VALUES {}
                                    '''.format("(?, ?)"), [line['asin'], cat])
                            except:
                                print("something went wrong with ", line)



                if 'related' in line.keys():
                    related = line['related']

                    # LIST OF ALSO VIEWED
                    if 'also_viewed' in related.keys():
                        for asin2 in related['also_viewed']:
                            try:
                                sqlcursor.execute('''
                                    INSERT INTO ALSOVIEWED
                                    VALUES {}
                                    '''.format("(?, ?)"), [line['asin'], asin2])
                            except:
                                print("something went wrong with ", line)

                    # LIST OF ALSO BOUGHT
                    if 'also_bought' in related.keys():
                        for asin2 in related['also_bought']:
                            try:
                                sqlcursor.execute('''
                                    INSERT INTO ALSOBOUGHT
                                    VALUES {}
                                    '''.format("(?, ?)"), [line['asin'], asin2])
                            except:
                                print("something went wrong with ", line)

                    # LIST OF BOUGHT TOGETHER
                    if 'bought_together' in related.keys():
                        for asin2 in related['bought_together']:
                            try:
                                sqlcursor.execute('''
                                    INSERT INTO BOUGHTTOGETHER
                                    VALUES {}
                                    '''.format("(?, ?)"), [line['asin'], asin2])
                            except:
                                print("something went wrong with ", line)

                    # LIST OF BUY AFTER VIEWING
                    if 'buy_after_viewing' in related.keys():
                        for asin2 in related['buy_after_viewing']:
                            try:
                                sqlcursor.execute('''
                                    INSERT INTO BUYAFTERVIEWING
                                    VALUES {}
                                    '''.format("(?, ?)"), [line['asin'], asin2])
                            except:
                                print("something went wrong with ", line)


                conn.commit()
                loops += 1
                total += 1
                if loops == 5000:
                    print("Finsihed", total, " in ", time.time() - start_time, " seconds")
                    loops = 0


def main():
    
    conn = sqlite3.connect('metadata.db')

    create_tables(conn, True)

    json_to_sql(conn, True, skip=9330000)

if __name__ == '__main__':
    main()
    print("---%s seconds ----" % (time.time() - start_time))



