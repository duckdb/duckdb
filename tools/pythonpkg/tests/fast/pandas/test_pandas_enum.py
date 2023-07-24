import pandas as pd
import pytest
import duckdb


class TestPandasEnum(object):
    def test_3480(self):
        con = duckdb.connect()
        con.execute(
            """
        create type cat as enum ('marie', 'duchess', 'toulouse');
        create table tab (
            cat cat,
            amt int
        );
        """
        )
        df = con.query(f"SELECT * FROM tab LIMIT 0;").to_df()
        print(df["cat"].cat.categories)
        assert df["cat"].cat.categories.equals(pd.Index(['marie', 'duchess', 'toulouse']))

    def test_3479(self):
        con = duckdb.connect()
        con.execute(
            """
        create type cat as enum ('marie', 'duchess', 'toulouse');
        create table tab (
            cat cat,
            amt int
        );
        """
        )

        df = pd.DataFrame(
            {
                "cat2": pd.Series(['duchess', 'toulouse', 'marie', None, "berlioz", "o_malley"], dtype="category"),
                "amt": [1, 2, 3, 4, 5, 6],
            }
        )
        con.register('df', df)
        with pytest.raises(
            duckdb.ConversionException,
            match='Type UINT8 with value 0 can\'t be cast because the value is out of range for the destination type UINT8',
        ):
            con.execute(f"INSERT INTO tab SELECT * FROM df;")

        assert con.execute("select * from tab").fetchall() == []
        con.execute("DROP TABLE tab")
        con.execute("DROP TYPE cat")
