import pytest
import duckdb
import pandas as pd

class TestMultipleColumnsSameName(object):

    def test_multiple_columns_with_same_name(self, duckdb_cursor):
        df = pd.DataFrame({'a': [1, 2, 3, 4], 'b': [5, 6, 7, 8], 'd': [9, 10, 11, 12]})
        df = df.rename(columns={ df.columns[1]: "a" })
        df_original = df.copy(True)
        con = duckdb.connect()
        con.register('df_view', df)

        assert con.execute("DESCRIBE df_view;").fetchall() == [('a', 'BIGINT', 'YES', None, None, None), ('a_1', 'BIGINT', 'YES', None, None, None), ('d', 'BIGINT', 'YES', None, None, None)]
        assert con.execute("select a_1 from df_view;").fetchall() == [(5,), (6,), (7,), (8,)]
        assert con.execute("select a from df_view;").fetchall() == [(1,), (2,), (3,), (4,)]
        # Verify we are not changing original dataframe
        assert df_original.equals(df)

    def test_multiple_columns_with_same_name_2(self, duckdb_cursor):
        df = pd.DataFrame({'a': [1, 2, 3, 4], 'b': [5, 6, 7, 8], 'a_1': [9, 10, 11, 12]})
        df = df.rename(columns={ df.columns[1]: "a_1" })
        con = duckdb.connect()
        con.register('df_view', df)
        assert con.execute("DESCRIBE df_view;").fetchall() == [('a', 'BIGINT', 'YES', None, None, None), ('a_1', 'BIGINT', 'YES', None, None, None), ('a_1_1', 'BIGINT', 'YES', None, None, None)]
        assert con.execute("select a_1 from df_view;").fetchall() == [(5,), (6,), (7,), (8,)]
        assert con.execute("select a from df_view;").fetchall() == [(1,), (2,), (3,), (4,)]

        assert con.execute("select a_1_1 from df_view;").fetchall() == [(9,), (10,), (11,), (12,)]
