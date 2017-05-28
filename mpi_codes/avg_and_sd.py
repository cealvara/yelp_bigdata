import os
import re
import sqlite3
import time

from mpi4py import MPI
from queue import PriorityQueue

comm = MPI.COMM_WORLD
size = MPI.COMM_WORLD.Get_size()
rank = MPI.COMM_WORLD.Get_rank()
name = MPI.Get_processor_name()


METADATA_DB = '/mnt/storage/metadata.db'

POSNEG_DB = '/mnt/storage/reviews_analysis.db'

JSON_PATH = '/mnt/local/data/'

ASIN_RE = re.compile(r"'asin': '(\w+)'")
CAT_RE = re.compile(r"meta_(\w+).json")


def get_values_from_file(filename):
    '''
    Reads a given file object and outputs
    tot_sum, N and list of values for SD
    '''
    f = open(filename, 'r')
    
    conn = sqlite3.connect(POSNEG_DB)

    c_posneg = conn.cursor()

    n = 0
    tot_score = 0
    tot_pos = 0
    tot_neg = 0
    list_score = []
    list_pos = []
    list_neg = []

    data = []

    for line in f:
        asin = ASIN_RE.search(line).group(1)
        if not asin:
            continue
        #get more info from raw metadata?
        
        query = c_posneg.execute('''
            select avg_score, total_prod_pos, total_prod_neg, total_prod_words 
            from scores 
            where asin=?;''', (asin,))
        
        data = query.fetchone()
        if not data:
            #print(line)
            continue
        
        avg_score, total_prod_pos, total_prod_neg, total_prod_words = data
        
        if total_prod_words == 0:
            continue

        avg_pos = total_prod_pos / total_prod_words
        avg_neg = total_prod_neg / total_prod_words

        list_score.append(avg_score)
        list_pos.append(avg_pos)
        list_neg.append(avg_neg)

        tot_score += avg_score
        tot_pos += avg_pos
        tot_neg += avg_neg

        n += 1

        data.append( (avg_score, avg_pos, avg_neg) )

    f.close()
    conn.close()

    return n, tot_score, tot_pos, tot_neg, list_score, list_pos, list_neg, data


if __name__ == '__main__':
    
    metadata_json_files = [f for f in os.listdir(JSON_PATH) if '.json' in f]
    
    if rank == 0:
        print(time.time())

    
    # GET STATS BY CATEGORY
    stat_by_category = {}
    
    all_data = []
    for filename in metadata_json_files:
        category = CAT_RE.search(filename).group(1)
        stat_by_category[category] = {}

        # each VM processes the file_range to get a list of values
        # (this is like a mapper)
        n, tot_score, tot_pos, tot_neg, list_score, list_pos, list_neg, data = get_values_from_file(filename)

        avg_score_cat = tot_score / n
        avg_pos_cat = tot_pos / n
        avg_neg_cat = tot_neg / n

        sd_score = sum([(l - avg_score_cat) ** 2 for l in list_score]) / n
        sd_pos = sum([(l - avg_pos_cat) ** 2 for l in list_pos]) / n
        sd_neg = sum([(l - avg_neg_cat) ** 2 for l in list_neg]) / n

        print(category, avg_score_cat, avg_pos_cat, avg_neg_cat, sd_score, sd_pos, sd_neg, n)

        all_data.extend(data)

    #root VM gathers all the chunks
    gathered_data = comm.gather(all_data, root=0)
    
    if rank == 0:
        df = pd.
    #    print(gathered_stat)
        print(time.time())

