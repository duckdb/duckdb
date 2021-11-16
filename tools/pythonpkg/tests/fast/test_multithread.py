import duckdb
import pytest
import threading
import queue as Queue

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

class TestDuckMultithread(object):

    def test_same_conn(self, duckdb_cursor):
        duck_threads = DuckDBThreaded(10,execute_query_same_connection)
        duck_threads.multithread_test(False)

    def test_execute(self, duckdb_cursor):
        duck_threads = DuckDBThreaded(10,execute_query)
        duck_threads.multithread_test()
 