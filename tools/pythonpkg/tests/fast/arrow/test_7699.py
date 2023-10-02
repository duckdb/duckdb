import duckdb
import pytest
import string

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")
pl = pytest.importorskip("polars")


class Test7699(object):
    def test_7699(self):
        pl_tbl = pl.DataFrame(
            {
                "col1": pl.Series([string.ascii_uppercase[ix + 10] for ix in list(range(2)) + list(range(3))]).cast(
                    pl.Categorical
                ),
            }
        )

        nickname = "df1234"
        duckdb.register(nickname, pl_tbl)

        rel = duckdb.sql("select * from df1234")
        res = rel.fetchall()
        assert res == [('K',), ('L',), ('K',), ('L',), ('M',)]
