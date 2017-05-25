import re
import sqlite3
import time

from mpi4py import MPI
from queue import PriorityQueue

comm = MPI.COMM_WORLD
size = MPI.COMM_WORLD.Get_size()
rank = MPI.COMM_WORLD.Get_rank()
name = MPI.Get_processor_name()

METADATA_JSON = '/mnt/storage/metadata.json'
METADATA_DB = '/mnt/storage/metadata.db'

# JACOB: PLEASE CREATE A NEW DB WITH TOM'S OUTPUT, CALLED "posneg.db"

POSNEG_DB = '/mnt/storage/posneg.db'

TOP_K_VALUE = 10

ASIN_RE = re.compile(r"'asin': '(\w+)'")

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


def get_values_for_sd(file_range, avg_by_category):
    outlist = None

    #here, outlist should be a list of tuples with:
    # (category name, sum of squared difference...)
    return outlist


if __name__ == '__main__':
    
    if rank == 0:
        print(time.time())
        file_ranges = get_file_ranges(METADATA_JSON, size)
        print(file_ranges, " from machine ", rank)
    else:
        file_ranges = None

    # each VM gets its own file_range to work with
    file_range = comm.scatter(file_ranges, root=0)
  

#STEP 1. GET AVERAGE BY CATEGORY

    # each VM processes the file_range to get a list of values
    # (this is like a mapper)
    list_of_keyvalues_for_avg = get_values_for_avg(file_range)

    #root VM gathers all the chunks
    gathered_chunks_for_avg = comm.gather(list_of_keyvalues_for_avg, root=0)

    if rank == 0:
        # process values from all chunks to get avg by category
        avg_by_category = f(gathered_chunks_for_avg)

#STEP 2. GET SD BY CATEGORY
    list_of_keyvalues_for_sd = get_values_for_sd(file_range, avg_by_category)

    #root VM gathers all the chunks
    gathered_chunks_for_sd = comm.gather(list_of_keyvalues_for_sd, root=0)

    if rank == 0:
        # process values from all chunks to get sd by category
        sd_by_category = g(gathered_chunks_for_avg)


    if rank == 0:
        print(time.time())
        print(avg_by_category)
        print(sd_by_category)       

