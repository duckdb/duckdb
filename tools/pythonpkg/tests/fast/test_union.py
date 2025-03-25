import duckdb
import pandas as pd


class TestUnion(object):
    def test_union_by_all(self):
        connection = duckdb.connect()

        connection.execute(
            """
			create table tbl1 as select * from (VALUES
				(1, 2, 3, 4),
				(2, 3, 4, 5),
				(3, 4, 5, 6)) as tbl(A, B, C, D)
		"""
        )
        connection.execute(
            """
			create table tbl2 as select * from (VALUES
				(11, 12, 13, 14, 15),
				(12, 13, 14, 15, 16),
				(13, 14, 15, 16, 17)) as tbl (A, B, C, D, E)
		"""
        )

        query = """
			select
				*
			from
				(
					select A, B, C, D, 0 as E from tbl1
				)
			union all (
				select * from tbl2
			) order by all
		"""
        res = connection.sql(query).fetchall()
        assert res == [
            (1, 2, 3, 4, 0),
            (2, 3, 4, 5, 0),
            (3, 4, 5, 6, 0),
            (11, 12, 13, 14, 15),
            (12, 13, 14, 15, 16),
            (13, 14, 15, 16, 17),
        ]

        df_1 = connection.execute("FROM tbl1").df()
        df_2 = connection.execute("FROM tbl2").df()

        query = """
			select
				*
			from
				(
					select A, B, C, D, 0 as E from df_1
				)
			union all (
				select * from df_2
			) order by all
		"""
        res = connection.sql(query).fetchall()
        assert res == [
            (1, 2, 3, 4, 0),
            (2, 3, 4, 5, 0),
            (3, 4, 5, 6, 0),
            (11, 12, 13, 14, 15),
            (12, 13, 14, 15, 16),
            (13, 14, 15, 16, 17),
        ]
