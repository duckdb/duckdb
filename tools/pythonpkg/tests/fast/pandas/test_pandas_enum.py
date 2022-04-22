import duckdb
import pandas as pd

class TestPandasEnum(object):
    def test_3480(self, duckdb_cursor):
        duckdb_cursor.execute(
        """
        create type cat as enum ('marie', 'duchess', 'toulouse');
        create table tab (
            cat cat,
            amt int
        );
        """
        )
        df = duckdb_cursor.query(f"SELECT * FROM tab LIMIT 0;").to_df()
        assert df["cat"].cat.categories.equals(pd.Index(['marie', 'duchess', 'toulouse']))


