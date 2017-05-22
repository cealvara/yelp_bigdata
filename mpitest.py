import ast
import sqlite3

from mpi4py import MPI
from queue import PriorityQueue

comm = MPI.COMM_WORLD
size = MPI.COMM_WORLD.Get_size()
rank = MPI.COMM_WORLD.Get_rank()
name = MPI.Get_processor_name()

JSON_PATH = '/mnt/storage/metadata.json'
DB_PATH = '/mnt/storage/metadata.db'
NUMROWS = 9430088
STEP = int(NUMROWS/size) + 1

TOP_K_VALUE = 3

if __name__ == '__main__':
    
    conn = sqlite3.connect(DB_PATH)

    c = conn.cursor()
    
    if rank == 0:
        json_data = open(JSON_PATH, 'r')
        
        child_rank = 1 % size
        while json_data:
            chunk_asin = []
            index = 0
            while index <= STEP and json_data:
                line = ast.literal_eval(json_data.readline())
                asin = line['asin']
                chunk_asin.append(asin)

            if child_rank != 0:
                comm.send(chunk_asin, dest=child_rank, tag=7)
                print('chunk {} sent!'.format(child_rank))
            
            child_rank = (child_rank + 1) % size

        json_data.close()
    
    else:
        chunk_asin = comm.recv(source=0, tag=7)

    outlist = PriorityQueue(maxsize=TOP_K_VALUE)

    for i, asin in enumerate(chunk_asin):
   
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

    gathered_chunks = comm.gather(outlist, root=0)
    
    if rank == 0:
        print(gathered_chunks)       

    conn.close()
