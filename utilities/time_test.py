import sqlite3
import time
import timeit

start_time = time.time()

def run():
	db = sqlite3.connect("metadata.db")
	c = db.cursor()
	for i in range(10000):
		r1 = c.execute('SELECT COUNT(asin2) FROM ALSOVIEWED WHERE asin = "B00JAL13CY";').fetchall() 
		r2 = c.execute('SELECT COUNT(asin2) FROM BUYAFTERVIEWING WHERE asin = "B00LDYFGFQ";').fetchall()
		r3 = c.execute('SELECT COUNT(asin2) FROM ALSOBOUGHT WHERE asin = "B00LGZD4BK";').fetchall()
		r4 = c.execute('SELECT COUNT(asin2) FROM ALSOBOUGHT WHERE asin = "B00L0Q3578";').fetchall()
		r5 = c.execute('SELECT COUNT(asin2) FROM BOUGHTTOGETHER WHERE asin = "B00L1PH402";').fetchall()
		r6 = c.execute('SELECT COUNT(asin2) FROM ALSOVIEWED WHERE asin = "B00LF3RQ5S";').fetchall()
		r7 = c.execute('SELECT COUNT(asin2) FROM ALSOVIEWED WHERE asin = "B00LDYFGFQ";').fetchall()
		# r8 = c.execute('SELECT DISTINCT COUNT(asin) FROM ALSOVIEWED ')

if __name__ == '__main__':
	run()
	print("Total runtime with Foerign: ", time.time() - start_time)