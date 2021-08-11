import duckdb
import pytest
try:
    import pyarrow as pa
    can_run = True
except:
    can_run = False

def check_equal(duckdb_conn):
    true_result = duckdb_conn.execute("SELECT * from test").fetchall()
    duck_tbl = duckdb_conn.table("test")
    duck_from_arrow = duckdb_conn.from_arrow_table(duck_tbl.arrow())
    duck_from_arrow.create("testarrow")
    arrow_result = duckdb_conn.execute("SELECT * from testarrow").fetchall()

class TestArrowFetch(object):
    def test_over_vector_size(self, duckdb_cursor):
        if not can_run:
            return

        duckdb_conn = duckdb.connect()
        duckdb_conn.execute("CREATE TABLE test (a  INTEGER)")
        for value in range (10000):
            duckdb_conn.execute("INSERT INTO  test VALUES ("+str(value) + ");")
        duckdb_conn.execute("INSERT INTO  test VALUES(NULL);")

        check_equal(duckdb_conn)

        assert(arrow_result == true_result)
    def test_empty_table(self, duckdb_cursor):
        if not can_run:
            return

        duckdb_conn = duckdb.connect()
        duckdb_conn.execute("CREATE TABLE test (a  INTEGER)")
     
        check_equal(duckdb_conn)

    def test_over_vector_size(self, duckdb_cursor):
        if not can_run:
            return

        duckdb_conn = duckdb.connect()

        duckdb_conn.execute("CREATE TABLE test (a  INTEGER)")
        for value in range (10000):
            duckdb_conn.execute("INSERT INTO  test VALUES ("+str(value) + ");")
        duckdb_conn.execute("INSERT INTO  test VALUES(NULL);")
        
        check_equal(duckdb_conn)

    def test_table_nulls(self, duckdb_cursor):
        if not can_run:
            return

        duckdb_conn = duckdb.connect()

        duckdb_conn.execute("CREATE TABLE test (a  INTEGER)")
        duckdb_conn.execute("INSERT INTO  test VALUES(NULL);")
        
        check_equal(duckdb_conn)

    def test_table_without_nulls(self, duckdb_cursor):
        if not can_run:
            return

        duckdb_conn = duckdb.connect()

        duckdb_conn.execute("CREATE TABLE test (a  INTEGER)")
        duckdb_conn.execute("INSERT INTO  test VALUES(1);")
        
        check_equal(duckdb_conn)

    def test_table_with_prepared_statements(self, duckdb_cursor):
        if not can_run:
            return

        duckdb_conn = duckdb.connect()

        duckdb_conn.execute("CREATE TABLE test (a  INTEGER)")
        duckdb_conn.execute("PREPARE s1 AS INSERT INTO test VALUES ($1), ($2 / 2)")

        for value in range (10000):
            duckdb_conn.execute("EXECUTE s1("+str(value) + "," + str(value*2)+  ");")

        check_equal(duckdb_conn)
