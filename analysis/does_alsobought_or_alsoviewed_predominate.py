#This program, meant to run with MPI, determines whether the alsoviewed or
#alsobought for a given product are more similar to the product. More
#specifically, it looks at all products for which there are at least 5
#alsobought and at least 5 alsoviewed (about 10% of the data) and sees
#which, when considered together, using model.docvecs.n_similarity, are more
#similar tot he product itself.
#
#The data is printed to screen in the format alsoviewed_dominate_count,
#alsobought_dominate_count, equal_similarity.
#
#The last of these happens when the set of alsoviewed equals the set of
#alsobought.

import gensim.models.doc2vec as d2v
import sqlite3, sys
import numpy as np
from mpi4py import MPI

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
	all_asins = master_cursor.execute("SELECT asin FROM metadata;")
	bought_dominant = 0
	viewed_dominant = 0
	equal = 0

for j in range(10):#Because we know how much data there is
	if rank == 0:
		collection = [all_asins.fetchone() for i in range(1000000)]
		chunks = np.array_split(collection, size)
	else:
		chunks = None
	chunk = comm.scatter(chunks, root = 0)
	node_bought_dominant = 0
	node_viewed_dominant = 0
	node_equal = 0
	need_to_break = False
	for asin_tuple in chunk:
		if need_to_break or asin_tuple == None:
			need_to_break = True
			continue
		asin = asin_tuple[0]
		alsobought = cursor.execute("SELECT asin2 FROM alsobought WHERE asin = '" + asin + "';")
		accepted_alsobought = [k[0] for k in alsobought if model.docvecs.__contains__(k[0])]
		if len(accepted_alsobought) < 5:
			continue#Too small
		alsoviewed = cursor.execute("SELECT asin2 FROM alsoviewed WHERE asin = '" + asin + "';")
		accepted_alsoviewed = [k[0] for k in alsoviewed if model.docvecs.__contains__(k[0])]
		if len(accepted_alsoviewed) < 5:
			continue#Too small
		try:
			viewed_similarity = model.docvecs.n_similarity([asin], accepted_alsoviewed)
			bought_similarity = model.docvecs.n_similarity([asin], accepted_alsobought)
		except KeyError:
			continue
		if viewed_similarity > bought_similarity:
			node_viewed_dominant += 1
		elif bought_similarity > viewed_similarity:
			node_bought_dominant += 1
		else:
			node_equal += 1
	gathered = comm.gather((node_viewed_dominant, node_bought_dominant, node_equal), root = 0)
	if rank == 0:
		for entry in gathered:
			viewed_dominant += entry[0]
			bought_dominant += entry[1]
			equal += entry[2]
	if need_to_break:
		break
if rank == 0:
	print(str(viewed_dominant) + ',' + str(bought_dominant) + "," + str(equal))
