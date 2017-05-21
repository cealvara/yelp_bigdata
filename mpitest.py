import sqlite3

from mpi4py import MPI

size = MPI.COMM_WORLD.Get_size()
rank = MPI.COMM_WORLD.Get_rank()
name = MPI.Get_processor_name()

DB_PATH = '/mnt/storage/metadata.db'

if __name__ == '__main__':
    
    conn = sqlite3.connect(DB_PATH)

    c = conn.cursor()

    asin_list = c.execute('''select asin from METADATA limit 10;''').fetchall()

    for asin in asin_list:
        c.execute('''select count(*) from ALSOVIEWED where asin=?;''', (asin[0],))

        for result in c:
            print(asin, result, rank, name)
