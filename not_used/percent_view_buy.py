import sqlite3
import time
start_time = time.time()

def main():
	db = sqlite3.connect("metadata.db")
	c = db.cursor()
	c2 = db.cursor()

	current_cat = None
	viewed_same = 0
	viewed_dif = 0
	bought_same = 0
	bought_dif = 0
	viewed_total = 0
	bought_total = 0
	buy_after_same = 0
	buy_after_diff = 0
	buy_after_total = 0

	loops = 0
	total = 0

	c.execute("SELECT asin FROM METADATA")
	for row in c:
		row = row[0]

		current_cat = c2.execute('''SELECT categories FROM CATEGORIES 
			WHERE asin == "{}";'''.format(row)).fetchall()

		current_cat = [f[0] for f in current_cat]
		# print("current cats: ", current_cat)
		also_viewed = c2.execute('''SELECT asin2 FROM ALSOVIEWED WHERE
			asin = "{}";'''.format(row)).fetchall()
		# print("also_viewed: ", also_viewed)
		also_bought = c2.execute('''SELECT asin2 FROM ALSOBOUGHT WHERE
			asin = "{}";'''.format(row)).fetchall()

		buy_after = c2.execute('''SELECT asin2 FROM BUYAFTERVIEWING WHERE
			asin = "{}";'''.format(row)).fetchall()

		# print("also_bought: ", also_bought)
		for asin2 in also_viewed:
			# print(asin2[0])
			asin2_cats = c2.execute('''SELECT categories FROM CATEGORIES
				WHERE asin = "{}"'''.format(asin2[0])).fetchall()
			# print("asin2 viewed cats: ", asin2_cats)
			for cat in asin2_cats:
				if cat[0] in current_cat:
					viewed_same += 1
				else:
					viewed_dif += 1
				viewed_total += 1
		for asin2 in also_bought:
			# print(asin2)
			asin2_cats = c2.execute('''SELECT categories FROM CATEGORIES
				WHERE asin = "{}"'''.format(asin2[0])).fetchall()
			# print("asin2 bought cats:", asin2_cats)
			for cat in asin2_cats:
				if cat[0] in current_cat:
					bought_same += 1
				else:
					bought_dif += 1
				bought_total += 1

		for asin2 in buy_after:
			asin2_cats = c2.execute('''SELECT categories FROM CATEGORIES
				WHERE asin = "{}"'''.format(asin2[0])).fetchall()
			for cat in asin2_cats:
				if cat[0] in current_cat:
					buy_after_same += 1
				else:
					buy_after_diff += 1
				buy_after_total += 1

		loops += 1
		total += 1
		if loops == 1000:
			print("Finished", total, " in ", time.time()-start_time, " seconds")
			loops = 0

	perecent_viewed = viewed_same/viewed_total
	diff_viewed = viewed_dif/viewed_total
	perecent_bought = bought_same/bought_total
	diff_bought = bought_dif/bought_total
	perecent_buy_after = buy_after_same/buy_after_total
	diff_buy_after = buy_after_diff/buy_after_total



	print("Percent Viewed = ", perecent_viewed)
	print("Difference in viewed = ", diff_viewed)
	print("Percent Bought = ", perecent_bought)
	print("Difference in bought = ", diff_bought)
	print("Percent Buy After Viewing = ", perecent_buy_after)
	print("Difference in buy after viewing = ", diff_buy_after)

	return


if __name__ == '__main__':
	main()


























