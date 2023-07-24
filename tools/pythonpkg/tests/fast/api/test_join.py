import duckdb
import pytest


class TestJoin(object):
    def test_alias_from_sql(self):
        con = duckdb.connect()
        rel1 = con.sql("SELECT 1 AS col1, 2 AS col2")
        rel2 = con.sql("SELECT 1 AS col1, 3 AS col3")

        rel = con.sql('select * from rel1 JOIN rel2 USING (col1)')
        rel.show()
        res = rel.fetchall()
        assert res == [(1, 2, 3)]

    def test_relational_join(self):
        con = duckdb.connect()

        rel1 = con.sql("SELECT 1 AS col1, 2 AS col2")
        rel2 = con.sql("SELECT 1 AS col1, 3 AS col3")

        rel = rel1.join(rel2, 'col1')
        res = rel.fetchall()
        assert res == [(1, 2, 3)]

    def test_relational_join_alias_collision(self):
        con = duckdb.connect()

        rel1 = con.sql("SELECT 1 AS col1, 2 AS col2").set_alias('a')
        rel2 = con.sql("SELECT 1 AS col1, 3 AS col3").set_alias('a')

        with pytest.raises(duckdb.InvalidInputException, match='Both relations have the same alias'):
            rel = rel1.join(rel2, 'col1')

    def test_relational_join_with_condition(self):
        con = duckdb.connect()

        rel1 = con.sql("SELECT 1 AS col1, 2 AS col2", alias='rel1')
        rel2 = con.sql("SELECT 1 AS col1, 3 AS col3", alias='rel2')

        # This makes a USING clause, which is kind of unexpected behavior
        rel = rel1.join(rel2, 'rel1.col1 = rel2.col1')
        rel.show()
        res = rel.fetchall()
        assert res == [(1, 2, 1, 3)]
