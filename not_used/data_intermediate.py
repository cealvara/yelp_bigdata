import sqlite3
import nltk

def main(conn):
    sqlcursor = conn.cursor()

    sqlcursor.execute('''
        SELECT * FROM  
            (SELECT business_id, review_id, text 
            FROM BUSINESS JOIN REVIEW USING (business_id) LIMIT 100)
        JOIN 
            (SELECT business_id, review_id, text 
            FROM BUSINESS JOIN REVIEW USING (business_id) LIMIT 100);
        ''')

    count = 0

    with open('intermediate_file.txt', 'w') as f:

        for result in sqlcursor:
            str_to_file = "|".join(list(result)).replace("\n", " ")
            
            f.write(str_to_file)
            f.write("\n")
            
            count += 1

    print(count)

if __name__ == '__main__':

    conn = sqlite3.connect('example.db')

    main(conn)