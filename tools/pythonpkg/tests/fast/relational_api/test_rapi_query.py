import duckdb
import pytest


@pytest.fixture()
def tbl_table():
    con = duckdb.default_connection
    con.execute("drop table if exists tbl")
    con.execute("create table tbl (i integer)")
    yield
    con.execute('drop table tbl')


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

    def test_query_table_unrelated(self, tbl_table):
        con = duckdb.default_connection
        rel = con.table("tbl")
        # Querying a table relation
        rel = rel.query("x", "select 5")
        result = rel.execute()
        assert result.fetchall() == [(5,)]

    def test_query_table_qualified(self):
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

    def test_query_non_select(self):
        con = duckdb.connect()
        rel = con.query("select [1,2,3,4]")
        rel.query("relation", "create table tbl as select * from relation")

        result = con.execute("select * from tbl").fetchall()
        assert result == [([1, 2, 3, 4],)]

    def test_query_non_select_fail(self):
        con = duckdb.connect()
        rel = con.query("select [1,2,3,4]")
        con.execute("create table tbl as select range(10)")
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

    def test_query_non_select_result(self):
        with pytest.raises(duckdb.ParserException, match="syntax error"):
            duckdb.query('selec 42')

        res = duckdb.query('explain select 42').fetchall()
        assert len(res) > 0

        res = duckdb.query('describe select 42::INT AS column_name').fetchall()
        assert res[0][0] == 'column_name'

        res = duckdb.query('create or replace table tbl_non_select_result(i integer)')
        assert res is None

        res = duckdb.query('insert into tbl_non_select_result values (42)')
        assert res is None

        res = duckdb.query('insert into tbl_non_select_result values (84) returning *').fetchall()
        assert res == [(84,)]

        res = duckdb.query('select * from tbl_non_select_result').fetchall()
        assert res == [(42,), (84,)]

        res = duckdb.query('insert into tbl_non_select_result select * from range(10000) returning *').fetchall()
        assert len(res) == 10000

        res = duckdb.query('show tables').fetchall()
        assert len(res) > 0

        res = duckdb.query('drop table tbl_non_select_result')
        assert res is None

    def test_replacement_scan_recursion(self):
        con = duckdb.connect()
        depth_limit = 1000
        import sys

        if sys.platform.startswith('win'):
            # With the default we reach a stack overflow in the CI
            depth_limit = 250
        con.execute(f"SET max_expression_depth TO {depth_limit}")
        rel = con.sql('select 42')
        rel = con.sql('select * from rel')
        with pytest.raises(duckdb.BinderException, match=f'Max expression depth limit of {depth_limit} exceeded'):
            con.sql('select * from rel')
