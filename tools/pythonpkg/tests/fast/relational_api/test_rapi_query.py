import duckdb
import pytest
import platform
import sys


@pytest.fixture()
def tbl_table():
    con = duckdb.default_connection
    con.execute("drop table if exists tbl CASCADE")
    con.execute("create table tbl (i integer)")
    yield
    con.execute('drop table tbl CASCADE')


class TestRAPIQuery(object):
    @pytest.mark.parametrize('steps', [1, 2, 3, 4])
    def test_query_chain(self, steps):
        con = duckdb.default_connection
        amount = int(1000000)
        rel = None
        for _ in range(steps):
            rel = con.query(f"select i from range({amount}::BIGINT) tbl(i)")
            amount = amount / 10
            rel = rel.query("rel", f"select * from rel limit {amount}")
        result = rel.execute()
        assert len(result.fetchall()) == amount

    @pytest.mark.parametrize('input', [[5, 4, 3], [], [1000]])
    def test_query_table(self, tbl_table, input):
        con = duckdb.default_connection
        rel = con.table("tbl")
        for row in input:
            rel.insert([row])
        # Querying a table relation
        rel = rel.query("x", "select * from x")
        result = rel.execute()
        assert result.fetchall() == [tuple([x]) for x in input]

    def test_query_table_basic(self, tbl_table):
        con = duckdb.default_connection
        rel = con.table("tbl")
        # Querying a table relation
        rel = rel.query("x", "select 5")
        result = rel.execute()
        assert result.fetchall() == [(5,)]

    def test_query_table_qualified(self, duckdb_cursor):
        con = duckdb.default_connection
        con.execute("create schema fff")

        # Create table in fff schema
        con.execute("create table fff.t2 as select 1 as t")
        assert con.table("fff.t2").fetchall() == [(1,)]

    def test_query_insert_into_relation(self, tbl_table):
        con = duckdb.default_connection
        rel = con.query("select i from range(1000) tbl(i)")
        # Can't insert into this, not a table relation
        with pytest.raises(duckdb.InvalidInputException):
            rel.insert([5])

    def test_query_non_select(self, duckdb_cursor):
        rel = duckdb_cursor.query("select [1,2,3,4]")
        rel.query("relation", "create table tbl as select * from relation")

        result = duckdb_cursor.execute("select * from tbl").fetchall()
        assert result == [([1, 2, 3, 4],)]

    def test_query_non_select_fail(self, duckdb_cursor):
        rel = duckdb_cursor.query("select [1,2,3,4]")
        duckdb_cursor.execute("create table tbl as select range(10)")
        # Table already exists
        with pytest.raises(duckdb.CatalogException):
            rel.query("relation", "create table tbl as select * from relation")

        # View referenced does not exist
        with pytest.raises(duckdb.CatalogException):
            rel.query("relation", "create table tbl as select * from not_a_valid_view")

    def test_query_table_unrelated(self, tbl_table):
        con = duckdb.default_connection
        rel = con.table("tbl")
        # Querying a table relation
        rel = rel.query("x", "select 5")
        result = rel.execute()
        assert result.fetchall() == [(5,)]

    def test_query_non_select_result(self, duckdb_cursor):
        with pytest.raises(duckdb.ParserException, match="syntax error"):
            duckdb_cursor.query('selec 42')

        res = duckdb_cursor.query('explain select 42').fetchall()
        assert len(res) > 0

        res = duckdb_cursor.query('describe select 42::INT AS column_name').fetchall()
        assert res[0][0] == 'column_name'

        res = duckdb_cursor.query('create or replace table tbl_non_select_result(i integer)')
        assert res is None

        res = duckdb_cursor.query('insert into tbl_non_select_result values (42)')
        assert res is None

        res = duckdb_cursor.query('insert into tbl_non_select_result values (84) returning *').fetchall()
        assert res == [(84,)]

        res = duckdb_cursor.query('select * from tbl_non_select_result').fetchall()
        assert res == [(42,), (84,)]

        res = duckdb_cursor.query('insert into tbl_non_select_result select * from range(10000) returning *').fetchall()
        assert len(res) == 10000

        res = duckdb_cursor.query('show tables').fetchall()
        assert len(res) > 0

        res = duckdb_cursor.query('drop table tbl_non_select_result')
        assert res is None

    def test_replacement_scan_recursion(self, duckdb_cursor):
        depth_limit = 1000

        if sys.platform.startswith('win') or platform.system() == "Emscripten":
            # With the default we reach a stack overflow in the CI for windows
            # and also outside of it for Pyodide
            depth_limit = 250

        duckdb_cursor.execute(f"SET max_expression_depth TO {depth_limit}")
        rel = duckdb_cursor.sql('select 42 a, 21 b')
        rel = duckdb_cursor.sql('select a+a a, b+b b from rel')
        other_rel = duckdb_cursor.sql('select a from rel')
        res = other_rel.fetchall()
        assert res == [(84,)]
