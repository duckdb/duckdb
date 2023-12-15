import pytest
import duckdb


class Test3324(object):
    def test_3324(self, duckdb_cursor):
        create_output = duckdb_cursor.execute(
            """
        create or replace table my_table as 
        select 'test1' as column1, 1 as column2, 'quack' as column3 
        union all
        select 'test2' as column1, 2 as column2, 'quacks' as column3 
        union all
        select 'test3' as column1, 3 as column2, 'quacking' as column3 
        """
        ).fetch_df()
        prepare_output = duckdb_cursor.execute(
            """
            prepare v1 as 
                select 
                    column1
                    , column2
                    , column3 
                from my_table
                where 
                    column1 = $1"""
        ).fetch_df()

        with pytest.raises(duckdb.BinderException, match="Unexpected prepared parameter"):
            duckdb_cursor.execute("""execute v1(?)""", 'test1').fetch_df()
        with pytest.raises(duckdb.BinderException, match="Unexpected prepared parameter"):
            duckdb_cursor.execute("""execute v1(?)""", ('test1',)).fetch_df()
