import duckdb
import pytest

class DuckDBThreaded:
    def __init__(self,duckdb_insert_thread_count=1):
        self.duckdb_insert_thread_count = duckdb_insert_thread_count
        self.threads = []

    def execute_query(self,duckdb_conn):
        output_df = duckdb_conn.execute('select my_column from (values (42), (84), (NULL), (128)) tbl(i)')

    def same_conn_test(self):
        duckdb_conn = duckdb.connect()

        for i in range(0,self.duckdb_insert_thread_count):
            self.threads.append(threading.Thread(target=self.execute_query, args=(duckdb_conn,),name='duckdb_thread_'+str(i)))

        for i in range(0,len(self.threads)):
            self.threads[i].start()
        
        for i in range(0,len(self.threads)):
            self.threads[i].join()
    

class TestDuckMultithread(object):
    def test_same_conn(self, duckdb_cursor):
        duck_threads = DuckDBThreaded(10)
        with pytest.raises(Exception):
            duck_threads.same_conn_test()