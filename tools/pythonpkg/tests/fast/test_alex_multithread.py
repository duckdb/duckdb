import duckdb
from threading import Thread, current_thread
import pandas as pd
import os

def insert_from_thread(duckdb_con):
    # Insert a row with the name of the thread
    duckdb_cursor = duckdb_con.cursor() # Make a cursor within the thread
    duckdb_cursor.check_same_thread(False)
    thread_name = str(current_thread().name)
    duckdb_cursor.execute("""INSERT INTO my_inserts VALUES (?)""", (thread_name,))

def insert_from_thread_second(duckdb_cursor):
    # Insert a row with the name of the thread
    thread_name = str(current_thread().name)
    duckdb_cursor.execute("""INSERT INTO my_inserts VALUES (?)""", (thread_name,))

def insert_from_thread_third(duck_conn):
    duckdb_cursor = duck_conn.cursor()
    thread_name = str(current_thread().name)
    duckdb_cursor.execute("""INSERT INTO my_inserts VALUES (?)""", (thread_name,))

class TestCursorMultithread(object):
    def test_alex_first(self, duckdb_cursor):
        duckdb_con = duckdb.connect(check_same_thread=False) # In Memory DuckDB
        duckdb_con.execute("""CREATE OR REPLACE TABLE my_inserts (thread_name varchar)""")

        thread_count = 3
        threads = []

        # Kick off multiple threads (in the same process) 
        # Pass in the same connection as an argument, and an object to store the results
        for i in range(thread_count):
            threads.append(Thread(target=insert_from_thread,
                                    args=(duckdb_con,),
                                    name='my_thread_'+str(i)))

        for i in range(thread_count):
            threads[i].start()

        for i in range(thread_count):
            threads[i].join()

        assert duckdb_con.execute("""SELECT * FROM my_inserts order by thread_name""").fetchall() == [('my_thread_0',), ('my_thread_1',), ('my_thread_2',)]

    def test_alex_second(self, duckdb_cursor):
        duckdb_con = duckdb.connect(check_same_thread=False) # In Memory DuckDB
        duckdb_con.execute("""CREATE OR REPLACE TABLE my_inserts (thread_name varchar)""")

        thread_count = 3
        threads = []
        cursors = []

        # Kick off multiple threads (in the same process) 
        # Pass in the same connection as an argument, and an object to store the results
        for i in range(thread_count):
            cursors.append(duckdb_con.cursor())
            threads.append(Thread(target=insert_from_thread_second,
                                    args=(cursors[i],),
                                    name='my_thread_'+str(i)))

        for i in range(thread_count):
            threads[i].start()

        for i in range(thread_count):
            threads[i].join()

        assert duckdb_con.execute("""SELECT * FROM my_inserts order by thread_name""").fetchall() == [('my_thread_0',), ('my_thread_1',), ('my_thread_2',)]

    def test_alex_third(self, duckdb_cursor):
        duckdb_con = duckdb.connect('another_test_10.duckdb', check_same_thread=False)
        duckdb_con.execute("""CREATE OR REPLACE TABLE my_inserts (thread_name varchar)""")


        thread_count = 3
        threads = []

        # Kick off multiple threads (in the same process) 
        # Pass in the same connection as an argument, and an object to store the results
        for i in range(thread_count):
            threads.append(Thread(target=insert_from_thread_third,
                                    args=(duckdb_con,),
                                    name='my_thread_'+str(i)))

        for i in range(thread_count):
            threads[i].start()

        for i in range(thread_count):
            threads[i].join()

        assert duckdb_con.execute("""SELECT * FROM my_inserts order by thread_name""").fetchall() == [('my_thread_0',), ('my_thread_1',), ('my_thread_2',)]
        duckdb_con.close()
        os.remove('another_test_10.duckdb')