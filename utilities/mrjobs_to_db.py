'''
This program creates a sqlite3 database from the output from amazon_mrjob_worddict.py
Reads a txt filename of the following format: 
product, [avg_score, total_review_count, total_prod_pos, total_prod_neg, total_prod_words, word_dict]
Outputs to revies_analysis.db with two tables: scores and word_dict
'''

import sqlite3
import os
import re
import time

start_time = time.time()


def create(conn):
	# Create tables in the database
	c = conn.cursor()
	c.execute('''DROP TABLE IF EXISTS scores''')
	c.execute('''DROP TABLE IF EXISTS word_dict''')

	c.execute('''
            CREATE TABLE IF NOT EXISTS scores (
            asin TEXT PRIMARY KEY,
            avg_score FLOAT,
            total_review_count INT,
            total_prod_pos INT,
            total_prod_neg INT,
            total_prod_words INT);
        ''')
	c.execute('''
            CREATE TABLE IF NOT EXISTS word_dict (
            asin TEXT REFERENCES scores(asin),
            year TEXT,
            total_pos INT,
            total_neg INT,
            total_words INT);
        ''')

	conn.commit()


def populate(conn):
	# Read the text file line by line and update the database accordingly
	filename = 'mrjobs_all_reviews.txt'
	filepath = os.path.join("./" + filename)
	sqlcursor = conn.cursor()

	with open(filepath, 'r') as f:

		loops = 0
		total = 0
		for line in f:
			one = r'"(.+)"\s+\[(\d+\.\d+), (\d+), (\d+), (\d+), (\d+), \{(["\d{4}": \[\d+, \d+, \d+\]]+)'
			values = re.match(one, line)
			if values:
				asin = values.group(1)
				avg_score = values.group(2)
				total_review_count = values.group(3)
				total_prod_pos = values.group(4)
				total_prod_neg = values.group(5)
				total_prod_words = values.group(6)

				try:
					sqlcursor.execute('''
						INSERT INTO scores
						VALUES {}
						'''.format("(?,?,?,?,?,?)"), [asin, avg_score, total_review_count, total_prod_pos, total_prod_neg, total_prod_words])
				except:
					print('something went wrong with: ', line)

				years = values.group(7)
				year_search = r'"(\d{4})": \[(\d+), (\d+), (\d+)\]'
				year_re = re.findall(year_search, years)

				for year in year_re:
					try:
						sqlcursor.execute('''
							INSERT INTO word_dict
							VALUES {}
							'''.format("(?,?,?,?,?)"), [asin] + list(year))
					except:
						print("something went wrong with years for asin: ", asin)

			else:
				print("Errors")
				print(line)

			conn.commit()
			loops += 1
			total += 1
			if loops == 5000:
				print("Finsihed", total, " in ", time.time() - start_time, " seconds")
				loops = 0

def index(conn):
	# Create an index for each table in the databse for faster querrying 
	print("creating index")
	c = conn.cursor()
	c.execute("CREATE INDEX i_w ON scores(asin);")
	c.execute("CREATE INDEX i_d ON word_dict(asin);")


def main():
	conn = sqlite3.connect('reviews_analysis.db')
	create(conn)
	populate(conn)
	index(conn)


if __name__ == '__main__':
	main()
	print("---%s seconds ----" % (time.time() - start_time))




















