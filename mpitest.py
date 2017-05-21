import ast
import sqlite3

from mpi4py import MPI

size = MPI.COMM_WORLD.Get_size()
rank = MPI.COMM_WORLD.Get_rank()
name = MPI.Get_processor_name()

JSON_PATH = '/mnt/storage/metadata.json'
DB_PATH = '/mnt/storage/metadata.db'
NUMROWS = 9430088
STEP = int(NUMROWS/size) + 1
offset = rank * STEP

if __name__ == '__main__':
    
    json_data = open(JSON_PATH, 'r')
    
    for _ in range(0, offset):
        next(json_data)
    
    conn = sqlite3.connect(DB_PATH)

    c = conn.cursor()
    
    for i, line in enumerate(json_data):
        line = ast.literal_eval(line)
        asin = line['asin']
    
        c.execute('''select count(*) from ALSOVIEWED where asin=?;''', (asin,))

        for result in c:
            print(asin, result, rank, name)

        if i > 3:
            break

    conn.close()
