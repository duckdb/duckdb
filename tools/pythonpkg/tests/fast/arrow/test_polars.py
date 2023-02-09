import duckdb
import pytest

class TestPolars(object):
    def test_polars(self,duckdb_cursor):
        pd = pytest.importorskip("pandas")
        pl = pytest.importorskip("polars")
        df = pd.DataFrame(
            {
                "A": [1, 2, 3, 4, 5],
                "fruits": ["banana", "banana", "apple", "apple", "banana"],
                "B": [5, 4, 3, 2, 1],
                "cars": ["beetle", "audi", "beetle", "beetle", "beetle"],
            }
        )
        # scan plus return a polars dataframe
        polars_df = pl.DataFrame(df)
        polars_result = duckdb.sql('SELECT * FROM polars_df').pl()
        pd.testing.assert_frame_equal(polars_df.to_pandas(), polars_result.to_pandas())

        # now do the same for a lazy dataframe
        lazy_df = polars_df.lazy()
        lazy_result = duckdb.sql('SELECT * FROM lazy_df').pl()
        pd.testing.assert_frame_equal(polars_df.to_pandas(), lazy_result.to_pandas())
