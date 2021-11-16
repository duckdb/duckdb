import duckdb
import pytest
import threading
import queue as Queue

class DuckDBThreaded:
    def __init__(self,duckdb_insert_thread_count,thread_function):
        self.duckdb_insert_thread_count = duckdb_insert_thread_count
        self.threads = []
        self.thread_function = thread_function
        
    def same_conn_test(self):
        duckdb_conn = duckdb.connect()
        queue = Queue.Queue()
        return_value = False

        for i in range(0,self.duckdb_insert_thread_count):
            self.threads.append(threading.Thread(target=self.thread_function, args=(duckdb_conn,queue),name='duckdb_thread_'+str(i)))

        for i in range(0,len(self.threads)):
            self.threads[i].start()
            if queue.get():
                return_value = True
        
        for i in range(0,len(self.threads)):
            self.threads[i].join()

        assert (return_value)

def execute_query_same_connection(duckdb_conn, queue):
    try:
        out = duckdb_conn.execute('select my_column from (values (42), (84), (NULL), (128)) tbl(i)')
        queue.put(False)
    except:
        queue.put(True)  

class TestDuckMultithread(object):

    def test_same_conn(self, duckdb_cursor):
        duck_threads = DuckDBThreaded(10,execute_query_same_connection)
        duck_threads.same_conn_test()
 