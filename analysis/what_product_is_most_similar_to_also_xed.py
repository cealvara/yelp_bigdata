#This program determines if the product that is most similar to all of the
#also[bought, viewed] of a given product is in fact the product itself.
#
#Because this operation takes a while, random selection is used. Change how
#many are selected by adjusting the to_query number below.
#
#Bear in mind that many prodcts (60%, I believe) don't have any alsobought
#or alsoviewed, so the to_query is a pretty lofty upper bound.
#
#The output is printed to std_out in the format number_queried,
#success_alsobought,success_alsoviewed where a success is one in which the most
#similar product is the product itself.

from mpi4py import MPI
import sqlite3
import gensim.models.doc2vec as d2v
import numpy as np

comm = MPI.COMM_WORLD
rank, size = comm.Get_rank(), comm.Get_size()

#This takes a while (~45 s), and uses up ~6.5 GB of memory, so
#make sure your computer can handle it
model = d2v.Doc2Vec.load('/mnt/storage/model.d2v')

connection = sqlite3.connect('/mnt/storage/metadata.db')
cursor = connection.cursor()

to_query = 18000

if rank == 0:
	master_connection = sqlite3.connect('/mnt/storage/metadata.db')
	master_cursor = master_connection.cursor()

	#A comment on why the following line is done in the manner it is:
	#
	#Querying random lines from a sqlite3 database is difficult.
	#First of all, the size of the databse is not obtainable (though in
	#this case we are already aware that it is ~9.4 million) and
	#choosing randomly is impossible without knowing this.
	#
	#Even wih our knowledge of the size of the database, we would have to
	#write queries like SELECT asin FROM metdata LIMIT 1 OFFSET x;
	#for the randomly chosen values of x - and this operation takes a long
	#time, and we would be doing it many, many times.
	#
	#Having ensured that querying all the asins would fit into memory
	#comfortably, we have determined this is the easiest way to get a 
	#random sample. Obviously, this method does not scale well, so be 
	#careful about using it for larger datasets.

	all_asins = master_cursor.execute("SELECT asin FROM metadata;").fetchall()
	to_choose = np.random.randint(0, len(all_asins), to_query)
	chosen_asins = [None for i in range(to_query)]
	for i in range(to_query):
		chosen_asins[i] = all_asins[to_choose[i]]
	success_alsobought = 0
	success_alsoviewed = 0
	chunks = np.array_split(chosen_asins, size)
else:
	chunks = None
chunk = comm.scatter(chunks, root = 0)
node_success_alsobought = 0
node_success_alsoviewed = 0
null_queries = 0
for asin_tuple in chunk:
	asin = asin_tuple[0]
	alsobought = cursor.execute("SELECT asin2 FROM alsobought WHERE asin = '" + asin + "';")
	alsoviewed = cursor.execute("SELECT asin2 FROM alsoviewed WHERE asin = '" + asin + "';")
	accepted_alsobought = [k[0] for k in alsobought if model.docvecs.__contains__(k[0])]
	accepted_alsoviewed = [k[0] for k in alsoviewed if model.docvecs.__contains__(k[0])]
	if len(accepted_alsobought) == 0 and len(accepted_alsoviewed) == 0:
		null_queries += 1
	else:
		if len(accepted_alsobought) > 0:
			most_similar_alsobought = model.docvecs.most_similar(positive = accepted_alsobought, topn = 1)
			if most_similar_alsobought[0][0] == asin:
				node_success_alsobought += 1
		if len(accepted_alsoviewed) > 0:
			most_similar_alsoviewed = model.docvecs.most_similar(positive = accepted_alsoviewed, topn = 1)
			if most_similar_alsoviewed[0][0] == asin:
				node_success_alsoviewed += 1
combined = (node_success_alsobought, node_success_alsoviewed, null_queries)
gathered = comm.gather(combined, root = 0)
if rank == 0:
	for triplet in gathered:
		success_alsobought += triplet[0]
		success_alsoviewed += triplet[1]
		to_query -= triplet[2]
	print(str(to_query) + ',' + str(success_alsobought) + ',' + str(success_alsoviewed))
