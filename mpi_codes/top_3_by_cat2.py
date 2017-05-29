import sqlite3
import time
import re
import os
from mpi4py import MPI

start_time = time.time()
METADATA_DB = './metadata.db'

ASIN_RE = re.compile(r"'asin': '(\w+)'")
CAT_RE = re.compile(r"meta_(\w+).json")
# CAT_RE = re.compile(r"(\w+).json")

comm = MPI.COMM_WORLD
size = MPI.COMM_WORLD.Get_size()
rank = MPI.COMM_WORLD.Get_rank()
name = MPI.Get_processor_name()


def get_values_for_avg(filename):
	f = open(filename, 'r')
	
	conn = sqlite3.connect(METADATA_DB)

	c = conn.cursor()

	also_viewed_cat = {}
	also_bought_cat = {}
	buy_after_cat = {}

	loops = 0
	total = 0

	for line in f:
		asin = ASIN_RE.search(line).group(1)

		if not asin:
			continue

		also_viewed = c.execute('''SELECT asin2 FROM ALSOVIEWED WHERE
			asin = "{}";'''.format(asin)).fetchall()
		also_bought = c.execute('''SELECT asin2 FROM ALSOBOUGHT WHERE
			asin = "{}";'''.format(asin)).fetchall()
		buy_after = c.execute('''SELECT asin2 FROM BUYAFTERVIEWING WHERE
			asin = "{}";'''.format(asin)).fetchall()

		for asin2 in also_viewed:
			asin2_cats = c.execute('''SELECT categories FROM CATEGORIES
				WHERE asin = "{}"'''.format(asin2[0])).fetchall()
			for cat in asin2_cats:
				cat = cat[0].encode("ascii", "ignore")
				if cat in also_viewed_cat.keys():
					also_viewed_cat[cat] += 1
				else:
					also_viewed_cat[cat] = 1

		for asin2 in also_bought:
			asin2_cats = c.execute('''SELECT categories FROM CATEGORIES
				WHERE asin = "{}"'''.format(asin2[0])).fetchall()
			for cat in asin2_cats:
				cat = cat[0].encode("ascii", "ignore")
				if cat in also_bought_cat.keys():
					also_bought_cat[cat] += 1
				else:
					also_bought_cat[cat] = 1

		for asin2 in buy_after:
			asin2_cats = c.execute('''SELECT categories FROM CATEGORIES
				WHERE asin = "{}"'''.format(asin2[0])).fetchall()
			for cat in asin2_cats:
				cat = cat[0].encode("ascii", "ignore")
				if cat in buy_after_cat.keys():
					buy_after_cat[cat] += 1
				else:
					buy_after_cat[cat] = 1

		loops += 1
		total += 1
		if loops == 5000:
			print("Node:", rank, "Finsihed", total, "From", filename, "in", (time.time() - start_time)/60, "minutes")
			loops = 0

	f.close()
	conn.close()

	return also_viewed_cat, also_bought_cat, buy_after_cat

if __name__ == '__main__':

	metadata_json_files = [f for f in os.listdir("./") if '.json' in f]


	if rank == 0:
		print(time.time())

	stat_by_category = {}

	for filename in metadata_json_files:
		category = CAT_RE.search(filename).group(1)

		also_viewed_cat, also_bought_cat, buy_after_cat = get_values_for_avg(filename)
		sorted_views = sorted(also_viewed_cat.items(), key = lambda x: x[1], reverse = True)[:3]
		sorted_bought = sorted(also_bought_cat.items(), key = lambda x: x[1], reverse = True)[:3]
		sorted_buy_after = sorted(buy_after_cat.items(), key = lambda x: x[1], reverse = True)[:3]
		
		stat_by_category[category] = {'also_viewed':sorted_views,'also_bought':sorted_bought,'buy_after_viewing':sorted_buy_after}
		print("Rank: ", rank, " Done with Category: ", category," Time: ", time.time() - start_time)
	print(stat_by_category)















