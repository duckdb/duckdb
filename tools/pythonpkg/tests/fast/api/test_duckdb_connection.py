import duckdb
import pytest
from conftest import NumpyPandas, ArrowPandas


def is_dunder_method(method_name: str) -> bool:
    if len(method_name) < 4:
        return False
    return method_name[:2] == '__' and method_name[:-3:-1] == '__'


# This file contains tests for DuckDBPyConnection methods,
# wrapped by the 'duckdb' module, to execute with the 'default_connection'
class TestDuckDBConnection(object):
    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_append(self, pandas):
        duckdb.execute("Create table integers (i integer)")
        df_in = pandas.DataFrame(
            {
                'numbers': [1, 2, 3, 4, 5],
            }
        )
        duckdb.append('integers', df_in)
        assert duckdb.execute('select count(*) from integers').fetchone()[0] == 5
        # cleanup
        duckdb.execute("drop table integers")

    def test_default_connection_from_connect(self):
        duckdb.sql('create or replace table connect_default_connect (i integer)')
        con = duckdb.connect(':default:')
        con.sql('select i from connect_default_connect')
        duckdb.sql('drop table connect_default_connect')
        with pytest.raises(duckdb.Error):
            con.sql('select i from connect_default_connect')

        # not allowed with additional options
        with pytest.raises(
            duckdb.InvalidInputException, match='Default connection fetching is only allowed without additional options'
        ):
            con = duckdb.connect(':default:', read_only=True)

    def test_arrow(self):
        pyarrow = pytest.importorskip("pyarrow")
        duckdb.execute("select [1,2,3]")
        result = duckdb.arrow()

    def test_begin_commit(self):
        duckdb.begin()
        duckdb.execute("create table tbl as select 1")
        duckdb.commit()
        res = duckdb.table("tbl")
        duckdb.execute("drop table tbl")

    def test_begin_rollback(self):
        duckdb.begin()
        duckdb.execute("create table tbl as select 1")
        duckdb.rollback()
        with pytest.raises(duckdb.CatalogException):
            # Table does not exist
            res = duckdb.table("tbl")

    def test_cursor(self):
        duckdb.execute("create table tbl as select 3")
        duckdb_cursor = duckdb.cursor()
        res = duckdb_cursor.table("tbl").fetchall()
        assert res == [(3,)]
        duckdb_cursor.execute("drop table tbl")
        with pytest.raises(duckdb.CatalogException):
            # 'tbl' no longer exists
            duckdb.table("tbl")

    def test_df(self):
        ref = [([1, 2, 3],)]
        duckdb.execute("select [1,2,3]")
        res_df = duckdb.fetch_df()
        res = duckdb.query("select * from res_df").fetchall()
        assert res == ref

    def test_duplicate(self):
        duckdb.execute("create table tbl as select 5")
        dup_conn = duckdb.duplicate()
        dup_conn.table("tbl").fetchall()
        duckdb.execute("drop table tbl")
        with pytest.raises(duckdb.CatalogException):
            dup_conn.table("tbl").fetchall()

    def test_execute(self):
        assert [([4, 2],)] == duckdb.execute("select [4,2]").fetchall()

    def test_executemany(self):
        # executemany does not keep an open result set
        # TODO: shouldn't we also have a version that executes a query multiple times with different parameters, returning all of the results?
        duckdb.execute("create table tbl (i integer, j varchar)")
        duckdb.executemany("insert into tbl VALUES (?, ?)", [(5, 'test'), (2, 'duck'), (42, 'quack')])
        res = duckdb.table("tbl").fetchall()
        assert res == [(5, 'test'), (2, 'duck'), (42, 'quack')]
        duckdb.execute("drop table tbl")

    def test_fetch_arrow_table(self):
        # Needed for 'fetch_arrow_table'
        pyarrow = pytest.importorskip("pyarrow")

        duckdb.execute("Create Table test (a integer)")

        for i in range(1024):
            for j in range(2):
                duckdb.execute("Insert Into test values ('" + str(i) + "')")
        duckdb.execute("Insert Into test values ('5000')")
        duckdb.execute("Insert Into test values ('6000')")
        sql = '''
        SELECT  a, COUNT(*) AS repetitions
        FROM    test
        GROUP BY a
        '''

        result_df = duckdb.execute(sql).df()

        arrow_table = duckdb.execute(sql).fetch_arrow_table()

        arrow_df = arrow_table.to_pandas()
        assert result_df['repetitions'].sum() == arrow_df['repetitions'].sum()
        duckdb.execute("drop table test")

    def test_fetch_df(self):
        ref = [([1, 2, 3],)]
        duckdb.execute("select [1,2,3]")
        res_df = duckdb.fetch_df()
        res = duckdb.query("select * from res_df").fetchall()
        assert res == ref

    def test_fetch_df_chunk(self):
        duckdb.execute("CREATE table t as select range a from range(3000);")
        query = duckdb.execute("SELECT a FROM t")
        cur_chunk = query.fetch_df_chunk()
        assert cur_chunk['a'][0] == 0
        assert len(cur_chunk) == 2048
        cur_chunk = query.fetch_df_chunk()
        assert cur_chunk['a'][0] == 2048
        assert len(cur_chunk) == 952
        duckdb.execute("DROP TABLE t")

    def test_fetch_record_batch(self):
        # Needed for 'fetch_arrow_table'
        pyarrow = pytest.importorskip("pyarrow")

        duckdb.execute("CREATE table t as select range a from range(3000);")
        duckdb.execute("SELECT a FROM t")
        record_batch_reader = duckdb.fetch_record_batch(1024)
        chunk = record_batch_reader.read_all()
        assert len(chunk) == 3000

    def test_fetchall(self):
        assert [([1, 2, 3],)] == duckdb.execute("select [1,2,3]").fetchall()

    def test_fetchdf(self):
        ref = [([1, 2, 3],)]
        duckdb.execute("select [1,2,3]")
        res_df = duckdb.fetchdf()
        res = duckdb.query("select * from res_df").fetchall()
        assert res == ref

    def test_fetchmany(self):
        assert [(0,), (1,)] == duckdb.execute("select * from range(5)").fetchmany(2)

    def test_fetchnumpy(self):
        numpy = pytest.importorskip("numpy")
        duckdb.execute("SELECT BLOB 'hello'")
        results = duckdb.fetchall()
        assert results[0][0] == b'hello'

        duckdb.execute("SELECT BLOB 'hello' AS a")
        results = duckdb.fetchnumpy()
        assert results['a'] == numpy.array([b'hello'], dtype=object)

    def test_fetchone(self):
        assert (0,) == duckdb.execute("select * from range(5)").fetchone()

    def test_from_arrow(self):
        assert None != duckdb.from_arrow

    def test_from_csv_auto(self):
        assert None != duckdb.from_csv_auto

    def test_from_df(self):
        assert None != duckdb.from_df

    def test_from_parquet(self):
        assert None != duckdb.from_parquet

    def test_from_query(self):
        assert None != duckdb.from_query

    def test_from_substrait(self):
        assert None != duckdb.from_substrait

    def test_get_substrait(self):
        assert None != duckdb.get_substrait

    def test_get_substrait_json(self):
        assert None != duckdb.get_substrait_json

    def test_get_table_names(self):
        assert None != duckdb.get_table_names

    def test_install_extension(self):
        assert None != duckdb.install_extension

    def test_load_extension(self):
        assert None != duckdb.load_extension

    def test_query(self):
        assert [(3,)] == duckdb.query("select 3").fetchall()

    def test_register(self):
        assert None != duckdb.register

    def test_register_relation(self):
        con = duckdb.connect()
        rel = con.sql('select [5,4,3]')
        con.register("relation", rel)

        con.sql("create table tbl as select * from relation")
        assert con.table('tbl').fetchall() == [([5, 4, 3],)]

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_relation_out_of_scope(self, pandas):
        def temporary_scope():
            # Create a connection, we will return this
            con = duckdb.connect()
            # Create a dataframe
            df = pandas.DataFrame({'a': [1, 2, 3]})
            # The dataframe has to be registered as well
            # making sure it does not go out of scope
            con.register("df", df)
            rel = con.sql('select * from df')
            con.register("relation", rel)
            return con

        con = temporary_scope()
        res = con.sql('select * from relation').fetchall()
        print(res)

    def test_table(self):
        con = duckdb.connect()
        con.execute("create table tbl as select 1")
        assert [(1,)] == con.table("tbl").fetchall()

    def test_table_function(self):
        assert None != duckdb.table_function

    def test_unregister(self):
        assert None != duckdb.unregister

    def test_values(self):
        assert None != duckdb.values

    def test_view(self):
        duckdb.execute("create view vw as select range(5)")
        assert [([0, 1, 2, 3, 4],)] == duckdb.view("vw").fetchall()
        duckdb.execute("drop view vw")

    def test_description(self):
        assert None != duckdb.description

    def test_close(self):
        assert None != duckdb.close

    def test_interrupt(self):
        assert None != duckdb.interrupt

    def test_wrap_coverage(self):
        con = duckdb.default_connection

        # Skip all of the initial __xxxx__ methods
        connection_methods = dir(con)
        filtered_methods = [method for method in connection_methods if not is_dunder_method(method)]
        for method in filtered_methods:
            # Assert that every method of DuckDBPyConnection is wrapped by the 'duckdb' module
            assert method in dir(duckdb)
