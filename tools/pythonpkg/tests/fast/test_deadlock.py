import duckdb
import pandas as pd
import pyarrow as pa
import numpy as np
import sys
np.set_printoptions(threshold=sys.maxsize)
import threading


def main():
    queue_tester = QueueTester(2)
    queue_tester.begin_benchmark()
    print('Finished with main()')

class QueueTester:
    def __init__(self,duckdb_insert_thread_count=1):
        self.duckdb_insert_thread_count = duckdb_insert_thread_count
        self.threads = []
        
        self.local_data_dict = {}
        self.insert_lock = threading.Lock()
    
    def begin_benchmark(self):
        print('Begin Benchmark')
        filename = ':memory:'
        duckdb_conn = duckdb.connect(filename)
        duckdb_conn.execute('CREATE TABLE test(i integer)')

        for i in range(0,self.duckdb_insert_thread_count):
            self.threads.append(threading.Thread(target=self.DEADLOCK_register_arrow_and_select, args=(duckdb_conn,),name='duckdb_insert_'+str(i)))
            # self.threads.append(threading.Thread(target=self.HALF_WORKING_register_arrow_and_select, args=(duckdb_conn,),name='duckdb_insert_'+str(i)))
            # self.threads.append(threading.Thread(target=self.WORKING_register_arrow_and_select, args=(duckdb_conn,),name='duckdb_insert_'+str(i)))


        for i in range(0,len(self.threads)):
            self.threads[i].start()
        
        for i in range(0,len(self.threads)):
            self.threads[i].join()
        
        final_output = duckdb_conn.execute("SELECT * from test").fetchdf()

        print('Done with all threads\n',final_output)

    def DEADLOCK_register_arrow_and_select(self,duckdb_conn):
        thread_name = threading.current_thread().getName()
        print('\n'+thread_name+' In register_arrow_and_select')

        self.local_data_dict[thread_name] = pa.Table.from_pydict({'my_column':pa.array([1,2,3,4,5],type=pa.int64())})

        print('\n'+thread_name+' About to register Arrow table')
        duckdb_conn.register_arrow("arrow_table_"+thread_name,self.local_data_dict[thread_name])

        # We never reach this point in either thread
        print('\n'+thread_name+' About to select with DuckDB')
        insert_string = 'select my_column from arrow_table_'+thread_name
        output_df = duckdb_conn.execute(insert_string).fetchdf()
        # print('\n'+thread_name+' Select statement completed with output of:\n'+output_df)
    
    def HALF_WORKING_register_arrow_and_select(self,duckdb_conn):
        thread_name = threading.current_thread().getName()
        print('\n'+thread_name+' In register_arrow_and_select')

        self.local_data_dict[thread_name] = pa.Table.from_pydict({'my_column':pa.array([1,2,3,4,5],type=pa.int64())})

        print('\n'+thread_name+' Acquire lock just to register Arrow table')
        with self.insert_lock:
            print('\n'+thread_name+' About to register Arrow table')
            duckdb_conn.register_arrow("arrow_table_"+thread_name,self.local_data_dict[thread_name])

        print('\n'+thread_name+' Release lock')        

        print('\n'+thread_name+' About to select with DuckDB')
        insert_string = 'insert into test select my_column from arrow_table_'+thread_name
        output_df = duckdb_conn.execute(insert_string).fetchdf()
        print('\n'+thread_name+' Select statement completed with output of:\n'+str(output_df))

    def WORKING_register_arrow_and_select(self,duckdb_conn):
        thread_name = threading.current_thread().getName()
        print('\n'+thread_name+' In register_arrow_and_select')

        self.local_data_dict[thread_name] = pa.Table.from_pydict({'my_column':pa.array([1,2,3,4,5],type=pa.int64())})

        print('\n'+thread_name+' Acquire lock just to register Arrow table')
        with self.insert_lock:
            print('\n'+thread_name+' About to register Arrow table')
            duckdb_conn.register_arrow("arrow_table_"+thread_name,self.local_data_dict[thread_name])

                

            print('\n'+thread_name+' About to select with DuckDB')
            insert_string = 'insert into test select my_column from arrow_table_'+thread_name
            output_df = duckdb_conn.execute(insert_string).fetchdf()
            print('\n'+thread_name+' Select statement completed with output of:\n'+str(output_df))  
        print('\n'+thread_name+' Release lock')      

if __name__ == '__main__':
    main()