#This program computes the average similarity for the alsoviewed across
#all products.
#
#It is super easy to change for alsobought.
#Where changes are necessary, a "CHANGE_ME" comment has been added for
#easy grepping.
#
#The output is the average printed to screen at the termination of
#the program.
#I would not recommend piping to a file as is, because many progress
#statements are printed too - though they could be easily removed.

from mpi4py import MPI
import sqlite3
import gensim.models.doc2vec as d2v
import numpy as np

comm = MPI.COMM_WORLD
rank, size = comm.Get_rank(), comm.Get_size()

#This takes a while (~45 s), and uses up ~6.5 GB of memory, so
#make sure your computer can handle it
print ("Model loading at node "+str(rank))
model = d2v.Doc2Vec.load('/mnt/storage/model.d2v')
print ("Model loaded at node "+str(rank))

if rank == 0:
	connection = sqlite3.connect('metadata.db')
	cursor = connection.cursor()
	executor = cursor.execute("SELECT * FROM alsoviewed;")#CHANGE_ME
	total_count = 0
	total_sum = 0.0

for i in range(149):#CHANGE_ME (to 130)
	#The number 149 is used because of prior knowledge about the size
	#of the dataset. 130 should be used for bought.
	#If you don't know the size of the dataset in advance, change this
	#to an infinite while loop that has a break statement when you start
	#to get None from the query. 
	if rank == 0:
		print ("About to launch round "+str(i + 1))
		collection = [executor.fetchone() for j in range(1000000)]
		chunks = np.array_split(collection, size)
	else:
		chunks = None
	chunk = comm.scatter(chunks, root = 0)
	results = []
	for pair in chunk:
		if pair == None:
			continue
		try:
			similarity = model.docvecs.similarity(pair[0], pair[1])
		except KeyError:#Because the dataset isn't complete
			continue
		if similarity != None:
			results.append(similarity)
	gathered_chunks = comm.gather(results, root = 0)
	if rank == 0:
		for c in gathered_chunks:
			total_count += len(c)
			total_sum += sum(c)

if rank == 0:
	print("AVERAGE: "+str(total_sum / total_count))
