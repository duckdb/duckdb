import duckdb
import pytest
import os

pl = pytest.importorskip("polars")


def polars_supports_capsule():
    from packaging.version import Version

    return Version(pl.__version__) >= Version('1.4.1')


@pytest.mark.skipif(
    not polars_supports_capsule(), reason='Polars version does not support the Arrow PyCapsule interface'
)
class TestArrowPyCapsule(object):
    def test_polars_pycapsule_scan(self, duckdb_cursor):
        class MyObject:
            def __init__(self, obj):
                self.obj = obj
                self.count = 0

            def __arrow_c_stream__(self, requested_schema=None):
                self.count += 1
                return self.obj.__arrow_c_stream__(requested_schema=requested_schema)

        df = pl.DataFrame({'a': [1, 2, 3, 4], 'b': [5, 6, 7, 8]})
        obj = MyObject(df)

        # Call the __arrow_c_stream__ from within DuckDB
        res = duckdb_cursor.sql("select * from obj")
        assert res.fetchall() == [(1, 5), (2, 6), (3, 7), (4, 8)]
        assert obj.count == 1

        # Call the __arrow_c_stream__ method and pass in the capsule instead
        capsule = obj.__arrow_c_stream__()
        res = duckdb_cursor.sql("select * from capsule")
        assert res.fetchall() == [(1, 5), (2, 6), (3, 7), (4, 8)]
        assert obj.count == 2

        # Ensure __arrow_c_stream__ accepts a requested_schema argument as noop
        capsule = obj.__arrow_c_stream__(requested_schema="foo")
        res = duckdb_cursor.sql("select * from capsule")
        assert res.fetchall() == [(1, 5), (2, 6), (3, 7), (4, 8)]
        assert obj.count == 3

    def test_capsule_roundtrip(self, duckdb_cursor):
        def create_capsule():
            conn = duckdb.connect()
            rel = conn.sql("select i, i+1, -i from range(100) t(i)")

            capsule = rel.__arrow_c_stream__()
            return capsule

        capsule = create_capsule()
        rel2 = duckdb_cursor.sql("select * from capsule")
        assert rel2.fetchall() == [(i, i + 1, -i) for i in range(100)]

    def test_consumer_interface_roundtrip(self, duckdb_cursor):
        def create_table():
            class MyTable:
                def __init__(self, rel, conn):
                    self.rel = rel
                    self.conn = conn

                def __arrow_c_stream__(self, requested_schema=None):
                    return self.rel.__arrow_c_stream__(requested_schema=requested_schema)

            conn = duckdb.connect()
            rel = conn.sql("select i, i+1, -i from range(100) t(i)")
            return MyTable(rel, conn)

        tbl = create_table()
        rel2 = duckdb_cursor.sql("select * from tbl")
        assert rel2.fetchall() == [(i, i + 1, -i) for i in range(100)]
