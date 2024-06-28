import duckdb
import os

try:
    import pyarrow as pa

    can_run = True
except:
    can_run = False


class Test2426(object):
    def test_2426(self, duckdb_cursor):
        if not can_run:
            return

        con = duckdb.connect()
        con.execute("Create Table test (a integer)")

        for i in range(1024):
            for j in range(2):
                con.execute("Insert Into test values ('" + str(i) + "')")
        con.execute("Insert Into test values ('5000')")
        con.execute("Insert Into test values ('6000')")
        sql = '''
        SELECT  a, COUNT(*) AS repetitions
        FROM    test
        GROUP BY a
        '''

        result_df = con.execute(sql).df()

        arrow_table = con.execute(sql).fetch_arrow_table()

        arrow_df = arrow_table.to_pandas()
        assert result_df['repetitions'].sum() == arrow_df['repetitions'].sum()
