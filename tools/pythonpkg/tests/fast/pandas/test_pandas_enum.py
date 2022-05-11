import pandas as pd
import pytest

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
        duckdb_cursor.execute("DROP TABLE tab")
        duckdb_cursor.execute("DROP TYPE cat")

    def test_3479(self, duckdb_cursor):
        duckdb_cursor.execute(
        """
        create type cat as enum ('marie', 'duchess', 'toulouse');
        create table tab (
            cat cat,
            amt int
        );
        """
        )

        df = pd.DataFrame({"cat2": pd.Series(['duchess', 'toulouse', 'marie', None, "berlioz", "o_malley"], dtype="category"), "amt": [1, 2, 3, 4, 5, 6]})
        duckdb_cursor.register('df', df)
        with pytest.raises(Exception):
            duckdb_cursor.execute(f"INSERT INTO tab SELECT * FROM df;")

        assert duckdb_cursor.execute("select * from tab").fetchall() == []
        duckdb_cursor.execute("DROP TABLE tab")
        duckdb_cursor.execute("DROP TYPE cat")

