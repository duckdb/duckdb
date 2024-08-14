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
        df = pl.DataFrame({'a': [1, 2, 3, 4], 'b': [5, 6, 7, 8]})
        capsule = df.__arrow_c_stream__()

        res = duckdb_cursor.sql("select * from capsule")
        assert res.fetchall() == [(1, 5), (2, 6), (3, 7), (4, 8)]

    def test_capsule_roundtrip(self, duckdb_cursor):
        def create_capsule():
            conn = duckdb.connect()
            rel = conn.sql("select i, i+1, -i from range(100) t(i)")

            capsule = rel.__arrow_c_stream__()
            return capsule

        capsule = create_capsule()
        rel2 = duckdb_cursor.sql("select * from capsule")
        assert rel2.fetchall() == [(i, i + 1, -i) for i in range(100)]
