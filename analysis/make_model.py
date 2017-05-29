#This program builds the model.
#
#ARGUMENTS:
#ARGV[1]: The file with the reviews, in the format ASIN\tConcatenated Reviews
#ARGV[2]: Where to save the model.
#
#Some other things to tune:
#workers: How many workers the model should use when building.
#min_count: How many times a word need appear for it to be included in the
#	final model. Given the size of the corpus, a large min_count is a good
#	idea.
#
#Worth noting: On a 22-core machine, this took about 20 hours to run.


import gensim.models.doc2vec as d2v
import re, sys, logging

#Directly from: https://rare-technologies.com/deep-learning-with-word2vec-and-gensim/
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

class TaggedDocuments:
	def __init__(self, fileName):
		self.fileName = fileName
	def __iter__(self):
		f = open(fileName)
		for l in f:
			tabIndex = l.find('\t')
			asin = l[:tabIndex]
			text = l[tabIndex+1:]
			cleaned = re.sub('[^\w]', ' ', text).lower()
			yield d2v.TaggedDocument(words = re.split("\s*", cleaned), tags = [asin])
		f.close()

fileName = sys.argv[1]
modelName = sys.argv[2]
docs = TaggedDocuments(fileName)
model = d2v.Doc2Vec(docs, workers = 22, min_count = 80000)
#model.build_vocab(docs)
#model.train(docs, iter = model.epoch )
model.save(modelName)
