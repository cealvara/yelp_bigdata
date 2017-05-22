import ast
import re
import sqlite3
import time

from mpi4py import MPI
from queue import PriorityQueue

comm = MPI.COMM_WORLD
size = MPI.COMM_WORLD.Get_size()
rank = MPI.COMM_WORLD.Get_rank()
name = MPI.Get_processor_name()

JSON_PATH = '/mnt/storage/metadata.json'
DB_PATH = '/mnt/storage/metadata.db'

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

if __name__ == '__main__':
    
    conn = sqlite3.connect(DB_PATH)

    c = conn.cursor()
    
    if rank == 0:
        print(time.time())
        file_ranges = get_file_ranges(JSON_PATH, size)
        print(file_ranges, " from machine ", rank)
    else:
        file_ranges = None

    file_range = comm.scatter(file_ranges, root=0)
  
    json_data = open(JSON_PATH, 'r')
    outlist = PriorityQueue(maxsize=TOP_K_VALUE)

    curr_pos = file_range[0]
    json_data.seek(curr_pos)
    counter = 0
    while curr_pos < file_range[1]:
        line = json_data.readline()
        curr_pos = json_data.tell()

        try:
            asin = ASIN_RE.search(line).group(1)

            query = c.execute('''select count(asin2) from ALSOVIEWED where asin=?;''', (asin,))

            count = query.fetchone()[0]

            pair_info = (count, asin)

            if not outlist.full():
                outlist.put(pair_info)
            else:
                curr_min_info = outlist.get()
                if count >= curr_min_info[0]:
                    outlist.put(pair_info)
                else:
                    outlist.put(curr_min_info)

        except:
            print(line)

    outrv = []
    while not outlist.empty():
        outrv.append(outlist.get())
    print(outrv, "in machine", rank)

    gathered_chunks = comm.gather(outrv, root=0)

    if rank == 0:
        print(time.time())
        print(gathered_chunks)       

    conn.close()
