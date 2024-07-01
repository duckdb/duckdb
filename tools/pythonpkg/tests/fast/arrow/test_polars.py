import duckdb
import pytest

pl = pytest.importorskip("polars")
arrow = pytest.importorskip("pyarrow")
pl_testing = pytest.importorskip("polars.testing")


class TestPolars(object):
    def test_polars(self, duckdb_cursor):
        df = pl.DataFrame(
            {
                "A": [1, 2, 3, 4, 5],
                "fruits": ["banana", "banana", "apple", "apple", "banana"],
                "B": [5, 4, 3, 2, 1],
                "cars": ["beetle", "audi", "beetle", "beetle", "beetle"],
            }
        )
        # scan plus return a polars dataframe
        polars_result = duckdb_cursor.sql('SELECT * FROM df').pl()
        pl_testing.assert_frame_equal(df, polars_result)

        # now do the same for a lazy dataframe
        lazy_df = df.lazy()
        lazy_result = duckdb_cursor.sql('SELECT * FROM lazy_df').pl()
        pl_testing.assert_frame_equal(df, lazy_result)

        con = duckdb.connect()
        con_result = con.execute('SELECT * FROM df').pl()
        pl_testing.assert_frame_equal(df, con_result)

    def test_register_polars(self, duckdb_cursor):
        con = duckdb.connect()
        df = pl.DataFrame(
            {
                "A": [1, 2, 3, 4, 5],
                "fruits": ["banana", "banana", "apple", "apple", "banana"],
                "B": [5, 4, 3, 2, 1],
                "cars": ["beetle", "audi", "beetle", "beetle", "beetle"],
            }
        )
        # scan plus return a polars dataframe
        con.register('polars_df', df)
        polars_result = con.execute('select * from polars_df').pl()
        pl_testing.assert_frame_equal(df, polars_result)
        con.unregister('polars_df')
        with pytest.raises(duckdb.CatalogException, match='Table with name polars_df does not exist'):
            con.execute("SELECT * FROM polars_df;").pl()

        con.register('polars_df', df.lazy())
        polars_result = con.execute('select * from polars_df').pl()
        pl_testing.assert_frame_equal(df, polars_result)

    def test_empty_polars_dataframe(self, duckdb_cursor):
        polars_empty_df = pl.DataFrame()
        with pytest.raises(
            duckdb.InvalidInputException, match='Provided table/dataframe must have at least one column'
        ):
            duckdb_cursor.sql("from polars_empty_df")
