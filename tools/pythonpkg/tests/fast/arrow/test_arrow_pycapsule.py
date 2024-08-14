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

            def __arrow_c_stream__(self):
                self.count += 1
                return self.obj.__arrow_c_stream__()

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
