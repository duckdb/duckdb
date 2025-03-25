import duckdb
import pytest
import os
import pandas as pd

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")
ds = pytest.importorskip("pyarrow.dataset")


class TestArrowReplacementScan(object):
    def test_arrow_table_replacement_scan(self, duckdb_cursor):

        parquet_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data', 'userdata1.parquet')
        userdata_parquet_table = pq.read_table(parquet_filename)
        df = userdata_parquet_table.to_pandas()

        con = duckdb.connect()

        for i in range(5):
            assert con.execute("select count(*) from userdata_parquet_table").fetchone() == (1000,)
            assert con.execute("select count(*) from df").fetchone() == (1000,)

    @pytest.mark.skipif(
        not hasattr(pa.Table, '__arrow_c_stream__'),
        reason='This version of pyarrow does not support the Arrow Capsule Interface',
    )
    def test_arrow_pycapsule_replacement_scan(self, duckdb_cursor):
        tbl = pa.Table.from_pydict({'a': [1, 2, 3, 4, 5, 6, 7, 8, 9]})
        capsule = tbl.__arrow_c_stream__()

        rel = duckdb_cursor.sql("select * from capsule")
        assert rel.fetchall() == [(i,) for i in range(1, 10)]

        capsule = tbl.__arrow_c_stream__()
        rel = duckdb_cursor.sql("select * from capsule where a > 3 and a < 5")
        assert rel.fetchall() == [(4,)]

        tbl = pa.Table.from_pydict({'a': [1, 2, 3], 'b': [4, 5, 6], 'c': [7, 8, 9], 'd': [10, 11, 12]})
        capsule = tbl.__arrow_c_stream__()

        rel = duckdb_cursor.sql("select b, d from capsule")
        assert rel.fetchall() == [(i, i + 6) for i in range(4, 7)]

        with pytest.raises(duckdb.InvalidInputException, match='The ArrowArrayStream was already released'):
            rel = duckdb_cursor.sql("select b, d from capsule")

        schema_obj = tbl.schema
        schema_capsule = schema_obj.__arrow_c_schema__()
        with pytest.raises(
            duckdb.InvalidInputException, match="""Expected a 'arrow_array_stream' PyCapsule, got: arrow_schema"""
        ):
            rel = duckdb_cursor.sql("select b, d from schema_capsule")

    def test_arrow_table_replacement_scan_view(self, duckdb_cursor):

        parquet_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data', 'userdata1.parquet')
        userdata_parquet_table = pq.read_table(parquet_filename)

        con = duckdb.connect()

        con.execute("create view x as select * from userdata_parquet_table")
        del userdata_parquet_table
        with pytest.raises(duckdb.CatalogException, match='Table with name userdata_parquet_table does not exist'):
            assert con.execute("select count(*) from x").fetchone()

    def test_arrow_dataset_replacement_scan(self, duckdb_cursor):
        parquet_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data', 'userdata1.parquet')
        userdata_parquet_table = pq.read_table(parquet_filename)
        userdata_parquet_dataset = ds.dataset(parquet_filename)

        con = duckdb.connect()
        assert con.execute("select count(*) from userdata_parquet_dataset").fetchone() == (1000,)
