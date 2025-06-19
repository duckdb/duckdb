import duckdb
import pytest
from duckdb import (
    ColumnExpression,
    ConstantExpression,
    SQLExpression,
)


class TestSQLExpression(object):
    def test_sql_expression_basic(self, duckdb_cursor):

        # Test simple constant expressions
        expr = SQLExpression("42")
        rel = duckdb_cursor.sql("SELECT 1").select(expr)
        assert rel.fetchall() == [(42,)]

        expr = SQLExpression("'hello'")
        rel = duckdb_cursor.sql("SELECT 1").select(expr)
        assert rel.fetchall() == [('hello',)]

        expr = SQLExpression("NULL")
        rel = duckdb_cursor.sql("SELECT 1").select(expr)
        assert rel.fetchall() == [(None,)]

        # Test arithmetic expressions
        expr = SQLExpression("5 + 3")
        rel = duckdb_cursor.sql("SELECT 1").select(expr)
        assert rel.fetchall() == [(8,)]

        expr = SQLExpression("10 - 4")
        rel = duckdb_cursor.sql("SELECT 1").select(expr)
        assert rel.fetchall() == [(6,)]

        expr = SQLExpression("3 * 7")
        rel = duckdb_cursor.sql("SELECT 1").select(expr)
        assert rel.fetchall() == [(21,)]

        expr = SQLExpression("20 / 4")
        rel = duckdb_cursor.sql("SELECT 1").select(expr)
        assert rel.fetchall() == [(5.0,)]

        # Test function calls
        expr = SQLExpression("UPPER('test')")
        rel = duckdb_cursor.sql("SELECT 1").select(expr)
        assert rel.fetchall() == [('TEST',)]

        expr = SQLExpression("CONCAT('hello', ' ', 'world')")
        rel = duckdb_cursor.sql("SELECT 1").select(expr)
        assert rel.fetchall() == [('hello world',)]

    def test_sql_expression_with_columns(self, duckdb_cursor):

        # Create a test table
        duckdb_cursor.execute(
            """
            CREATE TABLE test_table(a INTEGER, b VARCHAR, c DOUBLE);
            INSERT INTO test_table VALUES
                (1, 'one', 1.1),
                (2, 'two', 2.2),
                (3, 'three', 3.3);
        """
        )

        # Test column references
        rel = duckdb_cursor.table("test_table")
        expr = SQLExpression("a")
        rel2 = rel.select(expr)
        assert rel2.fetchall() == [(1,), (2,), (3,)]

        # Test expressions with column references
        expr = SQLExpression("a * 10")
        rel2 = rel.select(expr)
        assert rel2.fetchall() == [(10,), (20,), (30,)]

        expr = SQLExpression("UPPER(b)")
        rel2 = rel.select(expr)
        assert rel2.fetchall() == [('ONE',), ('TWO',), ('THREE',)]

        # Test complex expressions
        expr = SQLExpression("CASE WHEN a > 1 THEN b ELSE 'default' END")
        rel2 = rel.select(expr)
        assert rel2.fetchall() == [('default',), ('two',), ('three',)]

        # Test combining with other expression types
        expr1 = SQLExpression("a + 5")
        expr2 = ColumnExpression("c")
        rel2 = rel.select(expr1, expr2)
        assert rel2.fetchall() == [(6, 1.1), (7, 2.2), (8, 3.3)]

    def test_sql_expression_errors(self, duckdb_cursor):
        # Test empty string
        with pytest.raises(duckdb.ParserException, match="SELECT clause without selection list"):
            SQLExpression("")

        # Test invalid SQL
        with pytest.raises(duckdb.ParserException, match='Parser Error: syntax error at or near "SELECT"'):
            SQLExpression("SELECT *")

        # Test multiple expressions
        with pytest.raises(
            duckdb.InvalidInputException,
            match="Please provide only a single expression to SQLExpression, found 2 expressions in the parsed string",
        ):
            SQLExpression("1, 2")

    def test_sql_expression_alias(self, duckdb_cursor):
        # Test aliasing
        expr = SQLExpression("42").alias("my_column")
        rel = duckdb_cursor.sql("SELECT 1").select(expr)
        assert rel.fetchall() == [(42,)]
        assert rel.columns[0] == "my_column"

        # Test with table
        duckdb_cursor.execute(
            """
            CREATE TABLE test_alias(a INTEGER, b VARCHAR);
            INSERT INTO test_alias VALUES(1, 'one'), (2, 'two');
        """
        )

        rel = duckdb_cursor.table("test_alias")
        expr = SQLExpression("a + 10").alias("a_plus_10")
        rel2 = rel.select(expr, "b")
        assert rel2.fetchall() == [(11, 'one'), (12, 'two')]
        assert rel2.columns == ['a_plus_10', 'b']

    def test_sql_expression_in_filter(self, duckdb_cursor):
        duckdb_cursor.execute(
            """
            CREATE TABLE filter_test(a INTEGER, b VARCHAR);
            INSERT INTO filter_test VALUES
                (1, 'one'),
                (2, 'two'),
                (3, 'three'),
                (4, 'four');
        """
        )

        rel = duckdb_cursor.table("filter_test")

        # Test filter with SQL expression
        expr = SQLExpression("a > 2")
        rel2 = rel.filter(expr)
        assert rel2.fetchall() == [(3, 'three'), (4, 'four')]

        # Test complex filter
        expr = SQLExpression("a % 2 = 0 AND b LIKE '%o%'")
        rel2 = rel.filter(expr)
        assert rel2.fetchall() == [(2, 'two'), (4, 'four')]

        # Test combining with other expression types
        expr1 = SQLExpression("a > 1")
        expr2 = ColumnExpression("b") == ConstantExpression("four")
        rel2 = rel.filter(expr1 & expr2)
        assert rel2.fetchall() == [(4, 'four')]

    def test_sql_expression_in_aggregates(self, duckdb_cursor):
        duckdb_cursor.execute(
            """
            CREATE TABLE agg_test(a INTEGER, b VARCHAR, c INTEGER);
            INSERT INTO agg_test VALUES
                (1, 'group1', 10),
                (2, 'group1', 20),
                (3, 'group2', 30),
                (4, 'group2', 40);
        """
        )

        rel = duckdb_cursor.table("agg_test")

        # Test simple aggregation
        expr = SQLExpression("SUM(a)")
        rel2 = rel.aggregate([expr])
        assert rel2.fetchall() == [(10,)]

        # Test aggregation with group by
        expr = SQLExpression("SUM(c)")
        rel2 = rel.aggregate([expr, "b"]).sort('b')
        result = rel2.fetchall()
        assert result == [(30, 'group1'), (70, 'group2')]

        # Test multiple aggregations
        expr1 = SQLExpression("SUM(a)").alias("sum_a")
        expr2 = SQLExpression("AVG(c)").alias("avg_c")
        rel2 = rel.aggregate([expr1, expr2], "b").sort('sum_a', 'avg_c')
        result = rel2.fetchall()
        result.sort()
        assert result == [(3, 15.0), (7, 35.0)]
