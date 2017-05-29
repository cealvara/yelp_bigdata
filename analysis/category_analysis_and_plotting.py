#This program takes in the json files produces by mpi_by_category2.py,
#builds a chart, and outputs some data.
#
#Ths importing format is off to make it run headlessly, and is taken from
#https://stackoverflow.com/questions/15061135/python-tkinter-probelm-using-with-ubuntu-server/15063719#15063719


import matplotlib
matplotlib.use('Agg')#To run it headlessly
from matplotlib import pyplot
pyplot.ioff()#To run it headlessly
import json
from scipy.stats import linregress as linear_regression

sum_alsobought_file = open('/mnt/storage/sum_by_category_alsobought.json')
sum_alsoviewed_file = open('/mnt/storage/sum_by_category_alsoviewed.json')
count_alsobought_file = open('/mnt/storage/count_by_category_alsobought.json')
count_alsoviewed_file = open('/mnt/storage/count_by_category_alsoviewed.json')

sum_alsobought = json.loads(sum_alsobought_file.readline())
sum_alsoviewed = json.loads(sum_alsoviewed_file.readline())
count_alsobought = json.loads(count_alsobought_file.readline())
count_alsoviewed = json.loads(count_alsoviewed_file.readline())

sum_alsobought_file.close()
sum_alsoviewed_file.close()
count_alsobought_file.close()
count_alsoviewed_file.close()

unfiltered_keys = set(sum_alsobought.keys()).intersection(set(sum_alsoviewed.keys()))
keys = filter(lambda key: count_alsobought[key] >= 10 and count_alsoviewed[key] >= 10, unfiltered_keys)
keys = list(keys)

average_alsobought = [sum_alsobought[key] / count_alsobought[key] for key in keys]
average_alsoviewed = [sum_alsoviewed[key] / count_alsoviewed[key] for key in keys]
also_bought_more_similar = [average_alsobought[i] > average_alsoviewed[i] for i in range(len(average_alsobought))]
also_bought_more_common = sum(also_bought_more_similar)
also_viewed_more_similar = [average_alsoviewed[i] > average_alsobought[i] for i in range(len(average_alsoviewed))]
also_viewed_more_common = sum(also_viewed_more_similar)
print("Also bought similarity > also viewed similarity in " + str(also_bought_more_common) + " of " + str(len(average_alsobought)) + " categories.")
print("Also viewed similarity > also bought similarity in " + str(also_viewed_more_common) + " of " + str(len(average_alsobought)) + " categories")
slope, intercept, _,_,_ = linear_regression(average_alsobought,average_alsoviewed)

pyplot.plot([0,1],[intercept, slope + intercept],'g',lw=4,label="Least-Squares Regression")
pyplot.plot([0,1],[0,1],'k',lw=4, label = "45-degree line")
pyplot.scatter(average_alsobought, average_alsoviewed)
axes = pyplot.axes()
axes.set_xlabel("Also-Bought Similarity by Category")
axes.set_ylabel("Also-Viewed Similarity by Category")
axes.set_xbound(0,1)
axes.set_ybound(0,1)
pyplot.legend()
pyplot.savefig('/mnt/storage/alsobought_versus_alsoviewed_similarity_by_category.png')
