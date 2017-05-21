from google.cloud import storage
from mrjob.job import MRJob

import json
import re

client = storage.Client()
bucket = client.get_bucket('data-cs123')

blob = bucket.get_blob('pos_words.txt')
pos_words = blob.download_as_string().decode("utf-8").splitlines()
pos_words = set(pos_words)

blob = bucket.get_blob('neg_words.txt')
neg_words = blob.download_as_string().decode("utf-8").splitlines()
neg_words = set(neg_words)

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

        for word in WORD_RE.findall(review_text):
            word_count += 1
            if word in pos_words:
                pos_count += 1
            if word in neg_words:
                neg_count += 1

        yield asin, [pos_count, neg_count, word_count, review_score, 1]

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

        yield product, [pos_count, neg_count, word_count, avg_score, review_count]


if __name__ == '__main__':

    AmazonReviewReduce.run()
