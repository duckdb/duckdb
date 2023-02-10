import duckdb
import pytest

class TestPolars(object):
    def test_polars(self,duckdb_cursor):
        pl = pytest.importorskip("polars")
        pl_testing = pytest.importorskip("polars.testing")
        df = pl.DataFrame(
            {
                "A": [1, 2, 3, 4, 5],
                "fruits": ["banana", "banana", "apple", "apple", "banana"],
                "B": [5, 4, 3, 2, 1],
                "cars": ["beetle", "audi", "beetle", "beetle", "beetle"],
            }
        )
        # scan plus return a polars dataframe
        polars_result = duckdb.sql('SELECT * FROM df').pl()
        pl_testing.assert_frame_equal(df, polars_result)

        # now do the same for a lazy dataframe
        lazy_df = df.lazy()
        lazy_result = duckdb.sql('SELECT * FROM lazy_df').pl()
        pl_testing.assert_frame_equal(df, lazy_result)

        con = duckdb.connect()
        con_result = con.execute('SELECT * FROM df').pl()
        pl_testing.assert_frame_equal(df, con_result)
