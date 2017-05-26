#This code, meant to run with MPI, determines how often for a given product
#the other product that is most similar to it is "also bought" or "also viewed"
#with it.
#
#Because the operation of determining which other product is a costly one
#(taking about 1.1 seconds each) this is done via random sampling. Change
#how many are sampled by adjusting the to_query number below.
#
#The data is output in the format to_query,found_alsobought,found_alsoviewed
#and appended to the file "found.both". If you want to change this, it is
#super easy to do so.

import gensim.models.doc2vec as d2v
import sqlite3, sys
import numpy as np
from mpi4py import MPI
from collections import defaultdict

comm = MPI.COMM_WORLD
rank, size = comm.Get_rank(), comm.Get_size()

#This takes a while (~45 s), and uses up ~6.5 GB of memory, so
#make sure your computer can handle it
model = d2v.Doc2Vec.load('model.d2v')

connection = sqlite3.connect('metadata.db')
cursor = connection.cursor()

to_query = 10000

if rank == 0:
	master_connection = sqlite3.connect('metadata.db')
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
	all_asins = master_cursor.execute("SELECT asin FROM metadata;").fetchall()#I've checked - this fits into memory
	to_choose = np.random.randint(0, len(all_asins), to_query)
	chosen_asins = [None for i in range(to_query)]
	for i in range(to_query):
		chosen_asins[i] = all_asins[to_choose[i]]
	found_alsobought = 0
	found_alsoviewed = 0
	chunks = np.array_split(chosen_asins, size)
else:
	chunks = None
chunk = comm.scatter(chunks, root = 0)
node_found_alsobought = 0
node_found_alsoviewed = 0
for asin_tuple in chunk:
	asin = asin_tuple[0]
	try:
		most_similar_asin = model.docvecs.most_similar(asin, topn = 1)[0][0]#This step takes a while... ~1.1 seconds per query
	except TypeError:#Because some things ended up in metadata.json
			 #withoutt having any reviews, causing this error
		to_query -=1
		continue
	in_alsobought = cursor.execute("SELECT asin FROM alsobought WHERE asin = '"+ asin +"' AND asin2 = '" + most_similar_asin + "';").fetchone()
	#If it is in, then this wil be something... o/w, it will be None
	#and hence evaluate to false
	if in_alsobought:
		node_found_alsobought += 1
	in_alsoviewed = cursor.execute("SELECT asin FROM alsoviewed WHERE asin = '" + asin + "' AND asin2 = '" + most_similar_asin + "';").fetchone()
	if in_alsoviewed:
		node_found_alsoviewed += 1
combined = (node_found_alsobought, node_found_alsoviewed)
gathered = comm.gather(combined, root = 0)
if rank == 0:
	for pair in gathered:
		found_alsobought += pair[0]
		found_alsoviewed += pair[1]
	f = open('found.both','a+')
	f.write(str(to_query) + "," + str(found_alsobought) + "," + str(found_alsoviewed) + '\n')
	f.close()
