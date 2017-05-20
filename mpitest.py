import sqlite3

from mpi4py import MPI

size = MPI.COMM_WORLD.Get_size()
rank = MPI.COMM_WORLD.Get_rank()
name = MPI.Get_processor_name()

conn = sqlite3.connect('/mnt/storage/metadata.db')

c = conn.cursor()

c.execute('''select count(*), category from SALESRANK group by category;''')

for result in c:
    print(result)
    
print("Hello from rank {0} of {1} on {2}".format(rank, size, name))