import duckdb
import pytest
import threading
import queue as Queue
import pandas as pd
import numpy as np
import os
try:
    import pyarrow as pa
    can_run = True
except:
    can_run = False

def connect_duck(duckdb_conn):
    out = duckdb_conn.execute('select i from (values (42), (84), (NULL), (128)) tbl(i)').fetchall()
    assert out == [(42,), (84,), (None,), (128,)]

class DuckDBThreaded:
    def __init__(self,duckdb_insert_thread_count,thread_function):
        self.duckdb_insert_thread_count = duckdb_insert_thread_count
        self.threads = []
        self.thread_function = thread_function
        
    def multithread_test(self,if_all_true=True):
        duckdb_conn = duckdb.connect()
        queue = Queue.Queue()
        return_value = False

        for i in range(0,self.duckdb_insert_thread_count):
            self.threads.append(threading.Thread(target=self.thread_function, args=(duckdb_conn,queue),name='duckdb_thread_'+str(i)))

        for i in range(0,len(self.threads)):
            self.threads[i].start()
            if not if_all_true:
                if queue.get():
                    return_value = True
            else:
                if i == 0 and queue.get():
                    return_value = True
                elif queue.get() and return_value:
                    return_value = True
            
        for i in range(0,len(self.threads)):
            self.threads[i].join()

        assert (return_value)


def execute_query_same_connection(duckdb_conn, queue):

    try:
        out = duckdb_conn.execute('select i from (values (42), (84), (NULL), (128)) tbl(i)')
        queue.put(False)
    except:
        queue.put(True)

def execute_query(duckdb_conn, queue):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    try:
        duckdb_conn.execute('select i from (values (42), (84), (NULL), (128)) tbl(i)')
        queue.put(True)
    except:
        queue.put(False)

def insert_runtime_error(duckdb_conn, queue):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    try:
        duckdb_conn.execute('insert into T values (42), (84), (NULL), (128)')
        queue.put(False)
    except:
        queue.put(True)  

def execute_many_query(duckdb_conn, queue):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    try:
        # from python docs
        duckdb_conn.execute('''CREATE TABLE stocks
             (date text, trans text, symbol text, qty real, price real)''')
        # Larger example that inserts many records at a time
        purchases = [('2006-03-28', 'BUY', 'IBM', 1000, 45.00),
                     ('2006-04-05', 'BUY', 'MSFT', 1000, 72.00),
                     ('2006-04-06', 'SELL', 'IBM', 500, 53.00),
                    ]
        duckdb_conn.executemany('INSERT INTO stocks VALUES (?,?,?,?,?)', purchases)
        queue.put(True)
    except:
        queue.put(False)  

def fetchone_query(duckdb_conn, queue):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    try:
        duckdb_conn.execute('select i from (values (42), (84), (NULL), (128)) tbl(i)').fetchone()
        queue.put(True)
    except:
        queue.put(False)  

def fetchall_query(duckdb_conn, queue):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    try:
        duckdb_conn.execute('select i from (values (42), (84), (NULL), (128)) tbl(i)').fetchall()
        queue.put(True)
    except:
        queue.put(False)  

def conn_close(duckdb_conn, queue):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    try:
        duckdb_conn.close()
        queue.put(True)
    except:
        queue.put(False)  

def fetchnp_query(duckdb_conn, queue):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    try:
        duckdb_conn.execute('select i from (values (42), (84), (NULL), (128)) tbl(i)').fetchnumpy()
        queue.put(True)
    except:
        queue.put(False) 

def fetchdf_query(duckdb_conn, queue):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    try:
        duckdb_conn.execute('select i from (values (42), (84), (NULL), (128)) tbl(i)').fetchdf()
        queue.put(True)
    except:
        queue.put(False)

def fetchdf_chunk_query(duckdb_conn, queue):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    try:
        duckdb_conn.execute('select i from (values (42), (84), (NULL), (128)) tbl(i)').fetch_df_chunk()
        queue.put(True)
    except:
        queue.put(False) 

def fetch_arrow_query(duckdb_conn, queue):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    try:
        duckdb_conn.execute('select i from (values (42), (84), (NULL), (128)) tbl(i)').fetch_arrow_table()
        queue.put(True)
    except:
        queue.put(False) 


def fetch_record_batch_query(duckdb_conn, queue):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    try:
        duckdb_conn.execute('select i from (values (42), (84), (NULL), (128)) tbl(i)').fetch_record_batch()
        queue.put(True)
    except:
        queue.put(False) 

def transaction_query(duckdb_conn, queue):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    duckdb_conn.execute("CREATE TABLE T ( i INTEGER)")
    try:
        duckdb_conn.begin()
        duckdb_conn.execute('insert into T values (42), (84), (NULL), (128)')
        duckdb_conn.rollback()
        duckdb_conn.execute('insert into T values (42), (84), (NULL), (128)')
        duckdb_conn.commit()
        queue.put(True)
    except:
        queue.put(False) 

def df_append(duckdb_conn, queue):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    duckdb_conn.execute("CREATE TABLE T ( i INTEGER)")
    df = pd.DataFrame(np.random.randint(0,100,size=15), columns=['A'])
    try:
        duckdb_conn.append('T',df)
        queue.put(True)
    except:
        queue.put(False) 

def df_register(duckdb_conn, queue):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    df = pd.DataFrame(np.random.randint(0,100,size=15), columns=['A'])
    try:
        duckdb_conn.register('T',df)
        queue.put(True)
    except:
        queue.put(False) 

def df_unregister(duckdb_conn, queue):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    df = pd.DataFrame(np.random.randint(0,100,size=15), columns=['A'])
    try:
        duckdb_conn.register('T',df)
        duckdb_conn.unregister('T')
        queue.put(True)
    except:
        queue.put(False) 

def arrow_register_unregister(duckdb_conn, queue):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    arrow_tbl = pa.Table.from_pydict({'my_column':pa.array([1,2,3,4,5],type=pa.int64())})
    try:
        duckdb_conn.register('T',arrow_tbl)
        duckdb_conn.unregister('T')
        queue.put(True)
    except:
        queue.put(False) 

def table(duckdb_conn, queue):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    duckdb_conn.execute("CREATE TABLE T ( i INTEGER)")
    try:
        out = duckdb_conn.table('T')
        queue.put(True)
    except:
        queue.put(False) 

def view(duckdb_conn, queue):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    duckdb_conn.execute("CREATE TABLE T ( i INTEGER)")
    duckdb_conn.execute("CREATE VIEW V as (SELECT * FROM T)")
    try:
        out = duckdb_conn.values([5, 'five'])
        queue.put(True)
    except:
        queue.put(False) 
def values(duckdb_conn, queue):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    try:
        out = duckdb_conn.values([5, 'five'])
        queue.put(True)
    except:
        queue.put(False) 


def from_query(duckdb_conn, queue):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    try:
        out = duckdb_conn.from_query("select i from (values (42), (84), (NULL), (128)) tbl(i)")
        queue.put(True)
    except:
        queue.put(False) 

def from_df(duckdb_conn, queue):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    df = pd.DataFrame(['bla', 'blabla']*10, columns=['A'])
    try:
        out = duckdb_conn.execute("select * from df").fetchall()
        queue.put(True)
    except:
        queue.put(False)

def from_arrow(duckdb_conn, queue):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    arrow_tbl = pa.Table.from_pydict({'my_column':pa.array([1,2,3,4,5],type=pa.int64())})
    try:
        out = duckdb_conn.from_arrow(arrow_tbl)
        queue.put(True)
    except:
        queue.put(False)

def from_csv_auto(duckdb_conn, queue):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    filename = os.path.join(os.path.dirname(os.path.realpath(__file__)),'data','integers.csv')
    try:
        out = duckdb_conn.from_csv_auto(filename)
        queue.put(True)
    except:
        queue.put(False)     

def from_parquet(duckdb_conn, queue):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    filename = os.path.join(os.path.dirname(os.path.realpath(__file__)),'data','binary_string.parquet')
    try:
        out = duckdb_conn.from_parquet(filename)
        queue.put(True)
    except:
        queue.put(False)

def description(duckdb_conn, queue):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    duckdb_conn.execute('CREATE TABLE test (i bool, j TIME, k VARCHAR)')
    duckdb_conn.execute("INSERT INTO test VALUES (TRUE, '01:01:01', 'bla' )")
    rel = duckdb_conn.table("test")
    res = rel.execute()
    try:
        res.description()
        queue.put(True)
    except:
        queue.put(False)          

def cursor(duckdb_conn, queue):
    # Get a new connection
    cx = duckdb_conn.cursor()  
    try:
        cx.execute('CREATE TABLE test (i bool, j TIME, k VARCHAR)')
        queue.put(False)
    except:
        queue.put(True)    

class TestDuckMultithread(object):

    def test_execute(self, duckdb_cursor):
        duck_threads = DuckDBThreaded(10,execute_query)
        duck_threads.multithread_test()

    def test_execute_many(self, duckdb_cursor):
        duck_threads = DuckDBThreaded(10,execute_many_query)
        duck_threads.multithread_test()

    def test_fetchone(self, duckdb_cursor):
        duck_threads = DuckDBThreaded(10,fetchone_query)
        duck_threads.multithread_test()

    def test_fetchall(self, duckdb_cursor):
        duck_threads = DuckDBThreaded(10,fetchall_query)
        duck_threads.multithread_test()

    def test_close(self, duckdb_cursor):
        duck_threads = DuckDBThreaded(10,conn_close)
        duck_threads.multithread_test()

    def test_fetchnp(self, duckdb_cursor):
        duck_threads = DuckDBThreaded(10,fetchnp_query)
        duck_threads.multithread_test()

    def test_fetchdf(self, duckdb_cursor):
        duck_threads = DuckDBThreaded(10,fetchdf_query)
        duck_threads.multithread_test()

    def test_fetchdfchunk(self, duckdb_cursor):
        duck_threads = DuckDBThreaded(10,fetchdf_chunk_query)
        duck_threads.multithread_test()

    def test_fetcharrow(self, duckdb_cursor):
        if not can_run:
            return
        duck_threads = DuckDBThreaded(10,fetch_arrow_query)
        duck_threads.multithread_test()

    def test_fetch_record_batch(self, duckdb_cursor):
        if not can_run:
            return
        duck_threads = DuckDBThreaded(10,fetch_record_batch_query)
        duck_threads.multithread_test()

    def test_transaction(self, duckdb_cursor):
        duck_threads = DuckDBThreaded(10,transaction_query)
        duck_threads.multithread_test()

    def test_df_append(self, duckdb_cursor):
        duck_threads = DuckDBThreaded(10,df_append)
        duck_threads.multithread_test()

    def test_df_register(self, duckdb_cursor):
        duck_threads = DuckDBThreaded(10,df_register)
        duck_threads.multithread_test()

    def test_df_unregister(self, duckdb_cursor):
        duck_threads = DuckDBThreaded(10,df_unregister)
        duck_threads.multithread_test()

    def test_arrow_register_unregister(self, duckdb_cursor):
        if not can_run:
            return
        duck_threads = DuckDBThreaded(10,arrow_register_unregister)
        duck_threads.multithread_test()

    def test_table(self, duckdb_cursor):
        duck_threads = DuckDBThreaded(10,table)
        duck_threads.multithread_test()

    def test_view(self, duckdb_cursor):
        duck_threads = DuckDBThreaded(10,view)
        duck_threads.multithread_test()

    def test_values(self, duckdb_cursor):
        duck_threads = DuckDBThreaded(10,values)
        duck_threads.multithread_test()
    
    def test_from_query(self, duckdb_cursor):
        duck_threads = DuckDBThreaded(10,from_query)
        duck_threads.multithread_test()

    def test_from_DF(self, duckdb_cursor):
        duck_threads = DuckDBThreaded(10,from_df)
        duck_threads.multithread_test() 

    def test_from_arrow(self, duckdb_cursor):
        if not can_run:
            return
        duck_threads = DuckDBThreaded(10,from_arrow)
        duck_threads.multithread_test()
 
    def test_from_csv_auto(self, duckdb_cursor):
        duck_threads = DuckDBThreaded(10,from_csv_auto)
        duck_threads.multithread_test()

    def test_from_parquet(self, duckdb_cursor):
        duck_threads = DuckDBThreaded(10,from_parquet)
        duck_threads.multithread_test()

    def test_description(self, duckdb_cursor):
        duck_threads = DuckDBThreaded(10,description)
        duck_threads.multithread_test()

    def test_cursor(self, duckdb_cursor):
        duck_threads = DuckDBThreaded(10,cursor)
        duck_threads.multithread_test(False)


