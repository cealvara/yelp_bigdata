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
offset = rank * STEP

TOP_K_VALUE = 3

if __name__ == '__main__':
    
    json_data = open(JSON_PATH, 'r')
    
    for _ in range(0, offset):
        next(json_data)
    
    conn = sqlite3.connect(DB_PATH)

    c = conn.cursor()
    
    outlist = PriorityQueue(maxsize=TOP_K_VALUE)

    for i, line in enumerate(json_data):
        line = ast.literal_eval(line)
        asin = line['asin']
    
        query = c.execute('''select count(asin2) from ALSOVIEWED where asin=?;''', (asin,))

        count = query.fetchone()[0]

        pair_info = (count, asin)

        if not outlist.full():
            self.outlist.put(pair_info)
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
