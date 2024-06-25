import pytest
import duckdb
import pandas as pd


class TestMultipleColumnsSameName(object):
    def test_multiple_columns_with_same_name(self, duckdb_cursor):
        df = pd.DataFrame({'a': [1, 2, 3, 4], 'b': [5, 6, 7, 8], 'd': [9, 10, 11, 12]})
        df = df.rename(columns={df.columns[1]: "a"})
        duckdb_cursor.register('df_view', df)

        assert duckdb_cursor.table("df_view").columns == ['a', 'a_1', 'd']
        assert duckdb_cursor.execute("select a_1 from df_view;").fetchall() == [(5,), (6,), (7,), (8,)]
        assert duckdb_cursor.execute("select a from df_view;").fetchall() == [(1,), (2,), (3,), (4,)]
        # Verify we are not changing original dataframe
        assert all(df.columns == ['a', 'a', 'd']), df.columns

    def test_multiple_columns_with_same_name_relation(self, duckdb_cursor):
        df = pd.DataFrame({'a': [1, 2, 3, 4], 'b': [5, 6, 7, 8], 'd': [9, 10, 11, 12]})
        df = df.rename(columns={df.columns[1]: "a"})
        rel = duckdb_cursor.from_df(df)
        assert rel.query("df_view", "DESCRIBE df_view;").fetchall() == [
            ('a', 'BIGINT', 'YES', None, None, None),
            ('a_1', 'BIGINT', 'YES', None, None, None),
            ('d', 'BIGINT', 'YES', None, None, None),
        ]

        assert rel.query("df_view", "select a_1 from df_view;").fetchall() == [(5,), (6,), (7,), (8,)]
        assert rel.query("df_view", "select a from df_view;").fetchall() == [(1,), (2,), (3,), (4,)]

        # Verify we are not changing original dataframe
        assert all(df.columns == ['a', 'a', 'd']), df.columns

    def test_multiple_columns_with_same_name_replacement_scans(self, duckdb_cursor):
        df = pd.DataFrame({'a': [1, 2, 3, 4], 'b': [5, 6, 7, 8], 'd': [9, 10, 11, 12]})
        df = df.rename(columns={df.columns[1]: "a"})
        assert duckdb_cursor.execute("select a_1 from df;").fetchall() == [(5,), (6,), (7,), (8,)]
        assert duckdb_cursor.execute("select a from df;").fetchall() == [(1,), (2,), (3,), (4,)]
        # Verify we are not changing original dataframe
        assert all(df.columns == ['a', 'a', 'd']), df.columns

    def test_3669(self, duckdb_cursor):
        df = pd.DataFrame([(1, 5, 9), (2, 6, 10), (3, 7, 11), (4, 8, 12)], columns=['a_1', 'a', 'a'])
        duckdb_cursor.register('df_view', df)
        assert duckdb_cursor.table("df_view").columns == ['a_1', 'a', 'a_2']
        assert duckdb_cursor.execute("select a_1 from df_view;").fetchall() == [(1,), (2,), (3,), (4,)]
        assert duckdb_cursor.execute("select a from df_view;").fetchall() == [(5,), (6,), (7,), (8,)]
        # Verify we are not changing original dataframe
        assert all(df.columns == ['a_1', 'a', 'a']), df.columns

    def test_minimally_rename(self, duckdb_cursor):
        df = pd.DataFrame(
            [(1, 5, 9, 13), (2, 6, 10, 14), (3, 7, 11, 15), (4, 8, 12, 16)], columns=['a_1', 'a', 'a', 'a_2']
        )
        duckdb_cursor.register('df_view', df)
        rel = duckdb_cursor.table("df_view")
        res = rel.columns
        assert res == ['a_1', 'a', 'a_2', 'a_2_1']
        assert duckdb_cursor.execute("select a_1 from df_view;").fetchall() == [(1,), (2,), (3,), (4,)]
        assert duckdb_cursor.execute("select a from df_view;").fetchall() == [(5,), (6,), (7,), (8,)]
        assert duckdb_cursor.execute("select a_2 from df_view;").fetchall() == [(9,), (10,), (11,), (12,)]
        assert duckdb_cursor.execute("select a_2_1 from df_view;").fetchall() == [(13,), (14,), (15,), (16,)]
        # Verify we are not changing original dataframe
        assert all(df.columns == ['a_1', 'a', 'a', 'a_2']), df.columns

    def test_multiple_columns_with_same_name_2(self, duckdb_cursor):
        df = pd.DataFrame({'a': [1, 2, 3, 4], 'b': [5, 6, 7, 8], 'a_1': [9, 10, 11, 12]})
        df = df.rename(columns={df.columns[1]: "a_1"})
        duckdb_cursor.register('df_view', df)
        assert duckdb_cursor.table("df_view").columns == ['a', 'a_1', 'a_1_1']
        assert duckdb_cursor.execute("select a_1 from df_view;").fetchall() == [(5,), (6,), (7,), (8,)]
        assert duckdb_cursor.execute("select a from df_view;").fetchall() == [(1,), (2,), (3,), (4,)]
        assert duckdb_cursor.execute("select a_1_1 from df_view;").fetchall() == [(9,), (10,), (11,), (12,)]

    def test_case_insensitive(self, duckdb_cursor):
        df = pd.DataFrame({'A_1': [1, 2, 3, 4], 'a_1': [9, 10, 11, 12]})
        duckdb_cursor.register('df_view', df)
        assert duckdb_cursor.table("df_view").columns == ['A_1', 'a_1_1']
        assert duckdb_cursor.execute("select a_1 from df_view;").fetchall() == [(1,), (2,), (3,), (4,)]
        assert duckdb_cursor.execute("select a_1_1 from df_view;").fetchall() == [(9,), (10,), (11,), (12,)]
