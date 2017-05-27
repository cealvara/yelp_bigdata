#This program computes the average similarity of alsoviewed by category.
#
#It can readily be changed to do the same for alsobought. Places that have to
#be changed are commented with "CHANGE_ME" for easy grepping.
#
#The output of this prgoram is two json files,
#"count_by_category_alsoviewed.json" and "sum_by_category_alsoviewed.json"
#which have the number of alsoviewed comparisons by category and the sum of
#the similarity scores by category. Then you can compute the average by
#category readily. The decision was made to output the two json files
#rather than one because it gives a bit more information, and because
#computing the average is so trivial given this information.

from mpi4py import MPI
import sqlite3, json
import gensim.models.doc2vec as d2v
import numpy as np
from collections import defaultdict

comm = MPI.COMM_WORLD
rank, size = comm.Get_rank(), comm.Get_size()

#This takes a while (~45 s), and uses up ~6.5 GB of memory, so
#make sure your computer can handle it
model = d2v.Doc2Vec.load('/mnt/storage/model.d2v')
connection = sqlite3.connect('/mnt/storage/metadata.db')
cursor = connection.cursor()
if rank == 0:
	master_connection = sqlite3.connect('/mnt/storage/metadata.db')
	master_cursor = master_connection.cursor()
	master_executor = master_cursor.execute("SELECT asin, asin2 from alsoviewed;")#CHANGE_ME
	count_dict = defaultdict(int)
	sum_dict = defaultdict(float)

for i in range(149):#CHANGE_ME (to 130)
	#The number 149 is used because of prior knowledge about the size
	#of the dataset. 130 should be used for bought.
	#If you don't know the size of the dataset in advance, change this
	#to an infinite while loop that has a break statement when you start
	#to get None from the query. 
	if rank == 0:
		collection = [master_executor.fetchone() for i in range(1000000)]
		chunks = np.array_split(collection, size)
	else:
		chunks = None
	chunk = comm.scatter(chunks, root = 0)
	node_count_dict = defaultdict(int)
	node_sum_dict = defaultdict(float)
	for pair in chunk:
		if pair == None:
			break
		asin = pair[0]
		to_compare = pair[1]
		try:
			similarity = model.docvecs.similarity(asin, to_compare)
		except KeyError:#The model isn't complete
			continue
		category_information = cursor.execute("SELECT categories FROM categories WHERE asin ='" + asin + "';")
		for category_tuple in category_information:
			category = category_tuple[0]
			if category == '':
				category = "BLANK_CATEGORY"#For a pickle bug
			node_count_dict[category] += 1
			node_sum_dict[category] += similarity
	combined = (node_count_dict, node_sum_dict)
	gathered = comm.gather(combined, root = 0)
	if rank == 0:
		for pair in gathered:
			ncd = pair[0]
			nsd = pair[1]
			for key in ncd.keys():#The categories
				count_dict[key] += ncd[key]
				sum_dict[key] += nsd[key]
if rank == 0:
	f = open('count_by_category_alsoviewed.json', 'w')#CHANGE_ME
	f.write(json.dumps(count_dict))
	f.close()
	f = open('sum_by_category_alsoviewed.json', 'w')#CHANGE_ME
	f.write(json.dumps(sum_dict))
	f.close()
