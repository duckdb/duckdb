import duckdb
import pytest
from duckdb.typing import INTEGER, VARCHAR, TIMESTAMP
from duckdb import Expression, ConstantExpression, ColumnExpression, StarExpression, FunctionExpression, CaseExpression
from pyduckdb.value.constant import Value, IntegerValue
import datetime


class TestExpression(object):
    def test_constant_expression(self):
        con = duckdb.connect()

        val = Value(5, INTEGER)

        rel = con.sql(
            """
            select
                1 as a,
                2 as b,
                3 as c
        """
        )

        constant = ConstantExpression(val)

        rel = rel.select(constant)
        res = rel.fetchall()
        assert res == [(5,)]

    def test_column_expression(self):
        con = duckdb.connect()

        rel = con.sql(
            """
            select
                1 as a,
                2 as b,
                3 as c
        """
        )
        column = ColumnExpression('a')
        rel2 = rel.select(column)
        res = rel2.fetchall()
        assert res == [(1,)]

        column = ColumnExpression('d')
        with pytest.raises(duckdb.BinderException, match='Referenced column "d" not found'):
            rel2 = rel.select(column)

    def test_add_operator(self):
        con = duckdb.connect()

        val = Value(5, INTEGER)

        rel = con.sql(
            """
            select
                1 as a,
                2 as b,
                3 as c
        """
        )

        constant = ConstantExpression(val)
        col = ColumnExpression('b')
        expr = col + constant

        rel = rel.select(expr, expr)
        res = rel.fetchall()
        assert res == [(7, 7)]

    def test_binary_function_expression(self):
        con = duckdb.connect()

        rel = con.sql(
            """
            select
                1 as a,
                5 as b
        """
        )
        function = FunctionExpression("-", ColumnExpression('b'), ColumnExpression('a'))
        rel2 = rel.select(function)
        res = rel2.fetchall()
        assert res == [(4,)]

    def test_negate_expression(self):
        con = duckdb.connect()

        rel = con.sql(
            """
            select 5 as a
        """
        )
        col = ColumnExpression('a')
        col = -col
        rel = rel.select(col)
        res = rel.fetchall()
        assert res == [(-5,)]

    def test_subtract_expression(self):
        con = duckdb.connect()

        rel = con.sql(
            """
            select
                3 as a,
                1 as b
        """
        )
        col1 = ColumnExpression('a')
        col2 = ColumnExpression('b')
        expr = col1 - col2
        rel = rel.select(expr)
        res = rel.fetchall()
        assert res == [(2,)]

    def test_multiply_expression(self):
        con = duckdb.connect()

        rel = con.sql(
            """
            select
                3 as a,
                2 as b
        """
        )
        col1 = ColumnExpression('a')
        col2 = ColumnExpression('b')
        expr = col1 * col2
        rel = rel.select(expr)
        res = rel.fetchall()
        assert res == [(6,)]

    def test_division_expression(self):
        con = duckdb.connect()

        rel = con.sql(
            """
            select
                5 as a,
                2 as b
        """
        )
        col1 = ColumnExpression('a')
        col2 = ColumnExpression('b')
        expr = col1 / col2
        rel2 = rel.select(expr)
        res = rel2.fetchall()
        assert res == [(2.5,)]

        expr = col1 // col2
        rel2 = rel.select(expr)
        res = rel2.fetchall()
        assert res == [(2,)]

    def test_modulus_expression(self):
        con = duckdb.connect()

        rel = con.sql(
            """
            select
                5 as a,
                2 as b
        """
        )
        col1 = ColumnExpression('a')
        col2 = ColumnExpression('b')
        expr = col1 % col2
        rel2 = rel.select(expr)
        res = rel2.fetchall()
        assert res == [(1,)]

    def test_power_expression(self):
        con = duckdb.connect()

        rel = con.sql(
            """
            select
                5 as a,
                2 as b
        """
        )
        col1 = ColumnExpression('a')
        col2 = ColumnExpression('b')
        expr = col1**col2
        rel2 = rel.select(expr)
        res = rel2.fetchall()
        assert res == [(25,)]

    def test_equality_expression(self):
        con = duckdb.connect()

        rel = con.sql(
            """
            select
                5 as a,
                2 as b,
                5 as c
        """
        )
        col1 = ColumnExpression('a')
        col2 = ColumnExpression('b')
        col3 = ColumnExpression('c')
        expr1 = col1 == col2
        expr2 = col1 == col3
        rel2 = rel.select(expr1, expr2)
        res = rel2.fetchall()
        assert res == [(False, True)]

    def test_inequality_expression(self):
        con = duckdb.connect()

        rel = con.sql(
            """
            select
                5 as a,
                2 as b,
                5 as c
        """
        )
        col1 = ColumnExpression('a')
        col2 = ColumnExpression('b')
        col3 = ColumnExpression('c')
        expr1 = col1 != col2
        expr2 = col1 != col3
        rel2 = rel.select(expr1, expr2)
        res = rel2.fetchall()
        assert res == [(True, False)]

    def test_comparison_expressions(self):
        con = duckdb.connect()

        rel = con.sql(
            """
            select
                1 as a,
                2 as b,
                3 as c,
                3 as d
        """
        )
        col1 = ColumnExpression('a')
        col2 = ColumnExpression('b')
        col3 = ColumnExpression('c')
        col4 = ColumnExpression('d')

        # Greater than
        expr1 = col1 > col2
        expr2 = col2 > col1
        expr3 = col3 > col4
        rel2 = rel.select(expr1, expr2, expr3)
        res = rel2.fetchall()
        assert res == [(False, True, False)]

        # Greater than or equal
        expr1 = col1 >= col2
        expr2 = col2 >= col1
        expr3 = col3 >= col4
        rel2 = rel.select(expr1, expr2, expr3)
        res = rel2.fetchall()
        assert res == [(False, True, True)]

        # Less than
        expr1 = col1 < col2
        expr2 = col2 < col1
        expr3 = col3 < col4
        rel2 = rel.select(expr1, expr2, expr3)
        res = rel2.fetchall()
        assert res == [(True, False, False)]

        # Less than or equal
        expr1 = col1 <= col2
        expr2 = col2 <= col1
        expr3 = col3 <= col4
        rel2 = rel.select(expr1, expr2, expr3)
        res = rel2.fetchall()
        assert res == [(True, False, True)]

    def test_expression_alias(self):
        con = duckdb.connect()

        rel = con.sql(
            """
            select 1 as a
        """
        )
        col = ColumnExpression('a')
        col = col.alias('b')

        rel2 = rel.select(col)
        rel2.columns == ['b']

    def test_star_expression(self):
        con = duckdb.connect()

        rel = con.sql(
            """
            select
                1 as a,
                2 as b
        """
        )
        star = StarExpression()
        rel2 = rel.select(star)
        res = rel2.fetchall()
        assert res == [(1, 2)]

        # With exclude list
        star = StarExpression(exclude=['a'])
        rel2 = rel.select(star)
        res = rel2.fetchall()
        assert res == [(2,)]

    def test_struct_expression(self):
        con = duckdb.connect()

        rel = con.sql(
            """
            select
                1 as a,
                2 as b
        """
        )

        col1 = ColumnExpression('a')
        col2 = ColumnExpression('b')
        expr = FunctionExpression('struct_pack', col1, col2).alias('struct')

        rel = rel.select(expr)
        res = rel.fetchall()
        assert res == [({'a': 1, 'b': 2},)]

    def test_function_expression(self):
        con = duckdb.connect()

        def my_simple_func(a: int, b: int, c: int) -> int:
            return a + b + c

        con.create_function('my_func', my_simple_func)

        rel = con.sql(
            """
            select
                1 as a,
                2 as b,
                3 as c
        """
        )
        col1 = ColumnExpression('a')
        col2 = ColumnExpression('b')
        col3 = ColumnExpression('c')
        expr = FunctionExpression('my_func', col1, col2, col3)
        rel2 = rel.select(expr)
        res = rel2.fetchall()
        assert res == [(6,)]

    def test_case_expression(self):
        con = duckdb.connect()

        rel = con.sql(
            """
            select
                1 as a,
                2 as b,
                3 as c,
        """
        )

        col1 = ColumnExpression('a')
        col2 = ColumnExpression('b')
        col3 = ColumnExpression('c')

        const1 = ConstantExpression(IntegerValue(1))
        # CASE WHEN col1 > 1 THEN 5 ELSE NULL
        case1 = CaseExpression(col1 > const1, ConstantExpression(IntegerValue(5)))

        rel2 = rel.select(case1)
        res = rel2.fetchall()
        assert res == [(None,)]

        # CASE WHEN col1 > 1 THEN 5 WHEN col2 < col1 THEN 10 ELSE NULL
        case2 = case1.when(col2 < col1, ConstantExpression(IntegerValue(10)))
        rel2 = rel.select(case2)
        res = rel2.fetchall()
        assert res == [(None,)]

        # CASE WHEN col1 > 1 THEN 5 WHEN col2 < col1 THEN 10 ELSE 42
        case3 = case2.otherwise(ConstantExpression(IntegerValue(42)))
        rel2 = rel.select(case3)
        res = rel2.fetchall()
        assert res == [(42,)]

        # CASE WHEN col3 = col3 THEN 21 WHEN col3 > col1 THEN col3 ELSE col2
        case4 = (
            CaseExpression(col3 == col3, ConstantExpression(IntegerValue(21))).when(col3 > col1, col3).otherwise(col2)
        )
        rel2 = rel.select(case4)
        res = rel2.fetchall()
        assert res == [(21,)]

    def test_cast_expression(self):
        con = duckdb.connect()

        rel = con.sql("select '2022/01/21' as a")
        assert rel.types == [VARCHAR]

        col = ColumnExpression("a").cast(TIMESTAMP)
        rel = rel.select(col)
        assert rel.types == [TIMESTAMP]

        res = rel.fetchall()
        assert res == [(datetime.datetime(2022, 1, 21, 0, 0),)]

    def test_implicit_constant_conversion(self):
        con = duckdb.connect()
        rel = con.sql("select 42")
        res = rel.select(5).fetchall()
        assert res == [(5,)]

    def test_numeric_overflow(self):
        con = duckdb.connect()
        rel = con.sql('select 3000::SHORT salary')
        # If 100 is implicitly cast to TINYINT, the execution fails in an OverflowError
        expr = ColumnExpression("salary") * 100
        rel2 = rel.select(expr)
        res = rel2.fetchall()
        assert res == [(300_000,)]

        with pytest.raises(duckdb.OutOfRangeException, match="Overflow in multiplication of INT16"):
            import pyduckdb

            val = pyduckdb.Value(100, duckdb.typing.TINYINT)
            expr = ColumnExpression("salary") * val
            rel2 = rel.select(expr)
            res = rel2.fetchall()

    def test_struct_column_expression(self):
        con = duckdb.connect()
        rel = con.sql("select {'l': 1, 'ee': 33, 't': 7} as leet")
        expr = ColumnExpression("leet.ee")
        rel2 = rel.select(expr)
        res = rel2.fetchall()
        assert res == [(33,)]

    def test_filter(self):
        con = duckdb.connect()
        rel = con.sql("select * from (VALUES(1), (2), (1), (3)) tbl(a)")
        assert len(rel.fetchall()) == 4

        expr = ColumnExpression("a") == 1
        rel2 = rel.filter(expr)
        assert len(rel2.fetchall()) == 2
