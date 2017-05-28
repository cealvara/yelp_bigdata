#Use this program to make the file that is taken in by make_model.py
#
#This program reads in 'reviews.json' - which, importantly for the running of
#this program, is sorted by ASIN - concatenates all of the reivews, and
#outputs them to big_file.txt in the exact format that make_model.py likes.
#
#Keep in mind that big_file.txt will be a pretty big file.
#
#This program is not terribly sophisticated.

import json
f = open('reviews.json')#Conviniently - and importantly - sorted by asin
g = open('big_file.txt','w')#In the format of ASIN\tConcatenated Reviews
count = 0
current_asin = ''
first = True
for l in f:
	j = json.loads(l)
	text = j['reviewText']
	asin = j['asin']
	if asin == current_asin:
		g.write(' ' + text)
	else:
		current_asin = asin
		if first:
			first = False
		else:
			g.write('\n')
		g.write(asin + '\t' + text)
	if count % 100000 == 0:
		print (count)
	count += 1
g.write('\n')
g.close()
