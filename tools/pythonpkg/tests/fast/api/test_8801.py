import pytest
import duckdb


class Test8801(object):
    def test_8801(self):
        con = duckdb.connect()
        con.sql(
            """
            create table t (
                a int
            )
        """
        )
        rel = con.sql(
            """
            select * from (
                VALUES
                    (1),
                    (2)
                )
        """
        )
        # Fetch the first tuple
        res = rel.fetchone()
        # Execute a query containing multiple statements
        con.sql(
            """
            truncate t;
            insert into t values (1)
        """
        )
        # Fetch the new result
        rel2 = con.sql(
            """
            select * from t
        """
        )
        res = rel2.fetchall()
        assert res == [(1,)]
        other_res = rel.fetchall()
        # Fetch the remainder of the first result
        assert other_res == [(2,)]
