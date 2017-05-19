from mrjob.job import MRJob

import json
from google.cloud import storage
import re

WORD_RE = re.compile(r"[\w']+")

pos_words = []
gcs_file = gcs.open("gs://data-cs123/pos_words.txt")
for row in gcs_file:
    pos_words.append(row.rstrip())
gcs_file.close()
pos_words = set(pos_words)

neg_words = []
gcs_file = gcs.open("gs://data-cs123/neg_words.txt")
for row in gcs_file:
    neg_words.append(row.rstrip())
gcs_file.close()
neg_words = set(neg_words)


class AmazonReviewReduce(MRJob):

    def mapper(self, _, line):

        line = json.loads(line)
        
        reviewer_id = line.get("reviewerID", "")
        asin = line.get("asin", "")
        reviewer_name = line.get("reviewerName", "")
        helpful = line.get("helpful", "")
        review_text = line.get("reviewText", "")
        review_score = line.get("overall", "")
        review_summary = line.get("summary", "")
        unix_review_time = line.get("unixReviewTime", "")
        review_time = line.get("reviewTime", "")

        word_count = 0
        pos_count = 0
        neg_count = 0

        for word in WORD_RE.findall(review_text):
            word_count += 1
            if word in pos_words:
                pos_count += 1
            if word in neg_words:
                neg_count += 1

        yield asin, [pos_count, neg_count, word_count, review_score, 1]

        #to build out for time analysis-- pick products with the highest number of reviews 
        #and set up a binary variable to calculate if a review has above some threshold of 
        #positive reviews and/or above threshold of negative reviews. Create seasonal/monthly/
        #yearly buckets in order to capture the time of the review to assess trends in number of 
        #reviews, positive/negative reivews, etc. across the buckets. 

    def reducer(self, product, info):

        pos_count = 0
        neg_count = 0
        word_count = 0
        review_score = 0
        review_count = 0

        for review in info:
            pos_count += review[0]
            neg_count += review[1]
            word_count += review[2]
            review_score += review[3]
            review_count += review[4]

            avg_score = review_score/review_count

        #fix the reducer so that it doesn't iterate through a list
        #query the metadata here to grab the price, categories, etc.

        yield product, [pos_count, neg_count, word_count, avg_score, review_count]


if __name__ == '__main__':
    AmazonReviewReduce.run()