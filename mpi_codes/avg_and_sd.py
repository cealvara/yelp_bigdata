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

# JACOB: PLEASE CREATE A NEW DB WITH TOM'S OUTPUT, CALLED "posneg.db"

POSNEG_DB = '/mnt/storage/reviews_analysis.db'

ASIN_RE = re.compile(r"'asin': '(\w+)'")
CAT_RE = re.compile(r"meta_(\w+).json")

def get_file_ranges(fname, chunks):
    f = open(fname)

    fsize = f.seek(0,2)

    ranges = []
    chunk_size = fsize / chunks
    start = 0
    for i in range(chunks - 1):
        f.seek(start + chunk_size)
        l = f.readline()
        end = f.tell()
        ranges.append( (start, end) )
        start = end

    ranges.append( (start, fsize) )

    f.close()
    return ranges

def get_values_for_avg(file_range):
    base_data = open(METADATA_JSON, 'r')
    
    conn_metadata = sqlite3.connect(METADATA_DB)
    conn_posneg = sqlite3.connect(POSNEG_DB)
    
    c_metadata = conn_metadata.cursor()
    c_posneg = conn_posneg.cursor()

    outlist = []
    curr_pos = file_range[0]
    base_data.seek(curr_pos)

    while curr_pos < file_range[1]:
        line = base_data.readline()
        curr_pos = base_data.tell()

        asin = ASIN_RE.search(line).group(1)

        #here we should query both the metadata and the posneg databases
        query = c_metadata.execute('''select count(asin2) from ALSOVIEWED where asin=?;''', (asin,))

        count = query.fetchone()[0]

        pair_info = (asin, count)

        outlist.append(pair_info)

    base_data.close()
    conn_metadata.close()
    conn_posneg.close()

    #here, outlist should be a list of tuples with:
    # (category name, sum of positives, sum of negatives, ...)
    return outlist


def get_values_for_avg(filename):
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

    for line in f:
        asin = ASIN_RE.search(line).group(1)

        #get more info from raw metadata?
        
        query = c_posneg.execute('''
            select avg_score, total_prod_pos, total_prod_neg, total_prod_words 
            from scores 
            where asin=?;''', (asin,))
        
        data = query.fetchone()
        if not data:
            print(line)
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

    f.close()
    conn.close()

    return n, tot_score, tot_pos, tot_neg, list_score, list_pos, list_neg


if __name__ == '__main__':
    
    metadata_json_files = [f for f in os.listdir() if '.json' in f]
    
    if rank == 0:
        print(time.time())

    
    # GET STATS BY CATEGORY
    stat_by_category = {}
    
    for filename in metadata_json_files:
        category = CAT_RE.search(filename).group(1)
        stat_by_category[category] = {}

        # each VM processes the file_range to get a list of values
        # (this is like a mapper)
        n, tot_score, tot_pos, tot_neg, list_score, list_pos, list_neg = get_values_for_avg(filename)

        stat_by_category[category]['Mean'] = tot_score / n 

        stat_by_category[category]['SD'] = sum(
            [(l - tot_score / n) ** 2 for l in list_score]) / n


    #root VM gathers all the chunks
    gathered_stat = comm.gather(stat_by_category, root=0)
    
    if rank == 0:
        print(gathered_stat)
        print(time.time())

