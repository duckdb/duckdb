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
