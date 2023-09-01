import duckdb
import pytest
import threading
import queue as Queue
import numpy as np
from conftest import NumpyPandas, ArrowPandas
import os
from typing import List

try:
    import pyarrow as pa

    can_run = True
except:
    can_run = False


def connect_duck(duckdb_conn):
    out = duckdb_conn.execute('select i from (values (42), (84), (NULL), (128)) tbl(i)').fetchall()
    assert out == [(42,), (84,), (None,), (128,)]


def everything_succeeded(results: List[bool]):
    return all([result == True for result in results])


class DuckDBThreaded:
    def __init__(self, duckdb_insert_thread_count, thread_function, pandas):
        self.duckdb_insert_thread_count = duckdb_insert_thread_count
        self.threads = []
        self.thread_function = thread_function
        self.pandas = pandas

    def multithread_test(self, result_verification=everything_succeeded):
        duckdb_conn = duckdb.connect()
        queue = Queue.Queue()

        # Create all threads
        for i in range(0, self.duckdb_insert_thread_count):
            self.threads.append(
                threading.Thread(
                    target=self.thread_function, args=(duckdb_conn, queue, self.pandas), name='duckdb_thread_' + str(i)
                )
            )

        # Record for every thread if they succeeded or not
        thread_results = []
        for i in range(0, len(self.threads)):
            self.threads[i].start()
            thread_result: bool = queue.get(timeout=60)
            thread_results.append(thread_result)

        # Finish all threads
        for i in range(0, len(self.threads)):
            self.threads[i].join()

        # Assert that the results are what we expected
        assert result_verification(thread_results)


def execute_query_same_connection(duckdb_conn, queue, pandas):
    try:
        out = duckdb_conn.execute('select i from (values (42), (84), (NULL), (128)) tbl(i)')
        queue.put(False)
    except:
        queue.put(True)


def execute_query(duckdb_conn, queue, pandas):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    try:
        duckdb_conn.execute('select i from (values (42), (84), (NULL), (128)) tbl(i)')
        queue.put(True)
    except:
        queue.put(False)


def insert_runtime_error(duckdb_conn, queue, pandas):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    try:
        duckdb_conn.execute('insert into T values (42), (84), (NULL), (128)')
        queue.put(False)
    except:
        queue.put(True)


def execute_many_query(duckdb_conn, queue, pandas):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    try:
        # from python docs
        duckdb_conn.execute(
            """
                CREATE TABLE stocks(
                    date text,
                    trans text,
                    symbol text,
                    qty real,
                    price real
                )
            """
        )
        # Larger example that inserts many records at a time
        purchases = [
            ('2006-03-28', 'BUY', 'IBM', 1000, 45.00),
            ('2006-04-05', 'BUY', 'MSFT', 1000, 72.00),
            ('2006-04-06', 'SELL', 'IBM', 500, 53.00),
        ]
        duckdb_conn.executemany(
            """
            INSERT INTO stocks VALUES (?,?,?,?,?)
        """,
            purchases,
        )
        queue.put(True)
    except:
        queue.put(False)


def fetchone_query(duckdb_conn, queue, pandas):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    try:
        duckdb_conn.execute('select i from (values (42), (84), (NULL), (128)) tbl(i)').fetchone()
        queue.put(True)
    except:
        queue.put(False)


def fetchall_query(duckdb_conn, queue, pandas):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    try:
        duckdb_conn.execute('select i from (values (42), (84), (NULL), (128)) tbl(i)').fetchall()
        queue.put(True)
    except:
        queue.put(False)


def conn_close(duckdb_conn, queue, pandas):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    try:
        duckdb_conn.close()
        queue.put(True)
    except:
        queue.put(False)


def fetchnp_query(duckdb_conn, queue, pandas):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    try:
        duckdb_conn.execute('select i from (values (42), (84), (NULL), (128)) tbl(i)').fetchnumpy()
        queue.put(True)
    except:
        queue.put(False)


def fetchdf_query(duckdb_conn, queue, pandas):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    try:
        duckdb_conn.execute('select i from (values (42), (84), (NULL), (128)) tbl(i)').fetchdf()
        queue.put(True)
    except:
        queue.put(False)


def fetchdf_chunk_query(duckdb_conn, queue, pandas):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    try:
        duckdb_conn.execute('select i from (values (42), (84), (NULL), (128)) tbl(i)').fetch_df_chunk()
        queue.put(True)
    except:
        queue.put(False)


def fetch_arrow_query(duckdb_conn, queue, pandas):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    try:
        duckdb_conn.execute('select i from (values (42), (84), (NULL), (128)) tbl(i)').fetch_arrow_table()
        queue.put(True)
    except:
        queue.put(False)


def fetch_record_batch_query(duckdb_conn, queue, pandas):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    try:
        duckdb_conn.execute('select i from (values (42), (84), (NULL), (128)) tbl(i)').fetch_record_batch()
        queue.put(True)
    except:
        queue.put(False)


def transaction_query(duckdb_conn, queue, pandas):
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


def df_append(duckdb_conn, queue, pandas):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    duckdb_conn.execute("CREATE TABLE T ( i INTEGER)")
    df = pandas.DataFrame(np.random.randint(0, 100, size=15), columns=['A'])
    try:
        duckdb_conn.append('T', df)
        queue.put(True)
    except:
        queue.put(False)


def df_register(duckdb_conn, queue, pandas):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    df = pandas.DataFrame(np.random.randint(0, 100, size=15), columns=['A'])
    try:
        duckdb_conn.register('T', df)
        queue.put(True)
    except:
        queue.put(False)


def df_unregister(duckdb_conn, queue, pandas):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    df = pandas.DataFrame(np.random.randint(0, 100, size=15), columns=['A'])
    try:
        duckdb_conn.register('T', df)
        duckdb_conn.unregister('T')
        queue.put(True)
    except:
        queue.put(False)


def arrow_register_unregister(duckdb_conn, queue, pandas):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    arrow_tbl = pa.Table.from_pydict({'my_column': pa.array([1, 2, 3, 4, 5], type=pa.int64())})
    try:
        duckdb_conn.register('T', arrow_tbl)
        duckdb_conn.unregister('T')
        queue.put(True)
    except:
        queue.put(False)


def table(duckdb_conn, queue, pandas):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    duckdb_conn.execute("CREATE TABLE T ( i INTEGER)")
    try:
        out = duckdb_conn.table('T')
        queue.put(True)
    except:
        queue.put(False)


def view(duckdb_conn, queue, pandas):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    duckdb_conn.execute("CREATE TABLE T ( i INTEGER)")
    duckdb_conn.execute("CREATE VIEW V as (SELECT * FROM T)")
    try:
        out = duckdb_conn.values([5, 'five'])
        queue.put(True)
    except:
        queue.put(False)


def values(duckdb_conn, queue, pandas):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    try:
        out = duckdb_conn.values([5, 'five'])
        queue.put(True)
    except:
        queue.put(False)


def from_query(duckdb_conn, queue, pandas):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    try:
        out = duckdb_conn.from_query("select i from (values (42), (84), (NULL), (128)) tbl(i)")
        queue.put(True)
    except:
        queue.put(False)


def from_df(duckdb_conn, queue, pandas):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    df = pandas.DataFrame(['bla', 'blabla'] * 10, columns=['A'])
    try:
        out = duckdb_conn.execute("select * from df").fetchall()
        queue.put(True)
    except:
        queue.put(False)


def from_arrow(duckdb_conn, queue, pandas):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    arrow_tbl = pa.Table.from_pydict({'my_column': pa.array([1, 2, 3, 4, 5], type=pa.int64())})
    try:
        out = duckdb_conn.from_arrow(arrow_tbl)
        queue.put(True)
    except:
        queue.put(False)


def from_csv_auto(duckdb_conn, queue, pandas):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data', 'integers.csv')
    try:
        out = duckdb_conn.from_csv_auto(filename)
        queue.put(True)
    except:
        queue.put(False)


def from_parquet(duckdb_conn, queue, pandas):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data', 'binary_string.parquet')
    try:
        out = duckdb_conn.from_parquet(filename)
        queue.put(True)
    except:
        queue.put(False)


def description(duckdb_conn, queue, pandas):
    # Get a new connection
    duckdb_conn = duckdb.connect()
    duckdb_conn.execute('CREATE TABLE test (i bool, j TIME, k VARCHAR)')
    duckdb_conn.execute("INSERT INTO test VALUES (TRUE, '01:01:01', 'bla' )")
    rel = duckdb_conn.table("test")
    rel.execute()
    try:
        rel.description
        queue.put(True)
    except:
        queue.put(False)


def cursor(duckdb_conn, queue, pandas):
    # Get a new connection
    cx = duckdb_conn.cursor()
    try:
        cx.execute('CREATE TABLE test (i bool, j TIME, k VARCHAR)')
        queue.put(False)
    except:
        queue.put(True)


class TestDuckMultithread(object):
    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_execute(self, duckdb_cursor, pandas):
        duck_threads = DuckDBThreaded(10, execute_query, pandas)
        duck_threads.multithread_test()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_execute_many(self, duckdb_cursor, pandas):
        duck_threads = DuckDBThreaded(10, execute_many_query, pandas)
        duck_threads.multithread_test()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_fetchone(self, duckdb_cursor, pandas):
        duck_threads = DuckDBThreaded(10, fetchone_query, pandas)
        duck_threads.multithread_test()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_fetchall(self, duckdb_cursor, pandas):
        duck_threads = DuckDBThreaded(10, fetchall_query, pandas)
        duck_threads.multithread_test()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_close(self, duckdb_cursor, pandas):
        duck_threads = DuckDBThreaded(10, conn_close, pandas)
        duck_threads.multithread_test()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_fetchnp(self, duckdb_cursor, pandas):
        duck_threads = DuckDBThreaded(10, fetchnp_query, pandas)
        duck_threads.multithread_test()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_fetchdf(self, duckdb_cursor, pandas):
        duck_threads = DuckDBThreaded(10, fetchdf_query, pandas)
        duck_threads.multithread_test()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_fetchdfchunk(self, duckdb_cursor, pandas):
        duck_threads = DuckDBThreaded(10, fetchdf_chunk_query, pandas)
        duck_threads.multithread_test()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_fetcharrow(self, duckdb_cursor, pandas):
        if not can_run:
            return
        duck_threads = DuckDBThreaded(10, fetch_arrow_query, pandas)
        duck_threads.multithread_test()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_fetch_record_batch(self, duckdb_cursor, pandas):
        if not can_run:
            return
        duck_threads = DuckDBThreaded(10, fetch_record_batch_query, pandas)
        duck_threads.multithread_test()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_transaction(self, duckdb_cursor, pandas):
        duck_threads = DuckDBThreaded(10, transaction_query, pandas)
        duck_threads.multithread_test()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_df_append(self, duckdb_cursor, pandas):
        duck_threads = DuckDBThreaded(10, df_append, pandas)
        duck_threads.multithread_test()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_df_register(self, duckdb_cursor, pandas):
        duck_threads = DuckDBThreaded(10, df_register, pandas)
        duck_threads.multithread_test()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_df_unregister(self, duckdb_cursor, pandas):
        duck_threads = DuckDBThreaded(10, df_unregister, pandas)
        duck_threads.multithread_test()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_arrow_register_unregister(self, duckdb_cursor, pandas):
        if not can_run:
            return
        duck_threads = DuckDBThreaded(10, arrow_register_unregister, pandas)
        duck_threads.multithread_test()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_table(self, duckdb_cursor, pandas):
        duck_threads = DuckDBThreaded(10, table, pandas)
        duck_threads.multithread_test()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_view(self, duckdb_cursor, pandas):
        duck_threads = DuckDBThreaded(10, view, pandas)
        duck_threads.multithread_test()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_values(self, duckdb_cursor, pandas):
        duck_threads = DuckDBThreaded(10, values, pandas)
        duck_threads.multithread_test()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_from_query(self, duckdb_cursor, pandas):
        duck_threads = DuckDBThreaded(10, from_query, pandas)
        duck_threads.multithread_test()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_from_DF(self, duckdb_cursor, pandas):
        duck_threads = DuckDBThreaded(10, from_df, pandas)
        duck_threads.multithread_test()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_from_arrow(self, duckdb_cursor, pandas):
        if not can_run:
            return
        duck_threads = DuckDBThreaded(10, from_arrow, pandas)
        duck_threads.multithread_test()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_from_csv_auto(self, duckdb_cursor, pandas):
        duck_threads = DuckDBThreaded(10, from_csv_auto, pandas)
        duck_threads.multithread_test()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_from_parquet(self, duckdb_cursor, pandas):
        duck_threads = DuckDBThreaded(10, from_parquet, pandas)
        duck_threads.multithread_test()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_description(self, duckdb_cursor, pandas):
        duck_threads = DuckDBThreaded(10, description, pandas)
        duck_threads.multithread_test()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_cursor(self, duckdb_cursor, pandas):
        def only_some_succeed(results: List[bool]):
            if not any([result == True for result in results]):
                return False
            if all([result == True for result in results]):
                return False
            return True

        duck_threads = DuckDBThreaded(10, cursor, pandas)
        duck_threads.multithread_test(only_some_succeed)
