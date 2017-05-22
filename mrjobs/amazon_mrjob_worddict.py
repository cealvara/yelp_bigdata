from google.cloud import storage
from mrjob.job import MRJob

import json
import re

WORD_RE = re.compile(r"[\w']+")

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

        year = review_time[-4:]

        for word in WORD_RE.findall(review_text):
            word_count += 1
            if word in pos_words:
                pos_count += 1
            if word in neg_words:
                neg_count += 1

        yield asin, [year, pos_count, neg_count, word_count]


    def reducer(self, product, info):

        word_dict = {}

        total_prod_pos = 0
        total_prod_neg = 0
        total_prod_words = 0

        for review in info:

            year = review[0]
            pos_count = review[1]
            neg_count = review[2]
            total_word_count = review[3]

            total_prod_pos += pos_count
            total_prod_neg += neg_count
            total_prod_words += total_word_count

            if year not in word_dict:
                word_dict[year] = [0,0,0]

            word_dict[year][0] += pos_count
            word_dict[year][1] += neg_count
            word_dict[year][2] += total_word_count

        yield product, [total_prod_pos, total_prod_neg, total_prod_words, word_dict]

if __name__ == '__main__':
  
    AmazonReviewReduce.run()
