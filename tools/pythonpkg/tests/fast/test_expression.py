import platform
import duckdb
import pytest
from duckdb.typing import INTEGER, VARCHAR, TIMESTAMP
from duckdb import (
    Expression,
    ConstantExpression,
    ColumnExpression,
    LambdaExpression,
    CoalesceOperator,
    StarExpression,
    FunctionExpression,
    CaseExpression,
)
from duckdb.value.constant import Value, IntegerValue
import datetime

pytestmark = pytest.mark.skipif(
    platform.system() == "Emscripten",
    reason="Extensions are not supported on Emscripten",
)


@pytest.fixture(scope='function')
def filter_rel():
    con = duckdb.connect()
    rel = con.sql(
        """
        select * from (VALUES
            (1, 'a'),
            (2, 'b'),
            (1, 'b'),
            (3, 'c'),
            (4, 'a')
        ) tbl(a, b)
    """
    )
    yield rel


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

    @pytest.mark.skipif(platform.system() == 'Windows', reason="There is some weird interaction in Windows CI")
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

    def test_coalesce_operator(self):
        con = duckdb.connect()

        rel = con.sql(
            """
            select 'unused'
        """
        )

        rel2 = rel.select(CoalesceOperator(ConstantExpression(None), ConstantExpression('hello').cast(int)))
        res = rel2.explain()
        assert 'COALESCE' in res

        with pytest.raises(duckdb.ConversionException, match="Could not convert string 'hello' to INT64"):
            rel2.fetchall()

        con.execute(
            """
            CREATE TABLE exprtest(a INTEGER, b INTEGER);
            INSERT INTO exprtest VALUES (42, 10), (43, 100), (NULL, 1), (45, 0)
        """
        )

        with pytest.raises(duckdb.InvalidInputException, match='Please provide at least one argument'):
            rel3 = rel.select(CoalesceOperator())

        rel4 = rel.select(CoalesceOperator(ConstantExpression(None)))
        assert rel4.fetchone() == (None,)

        rel5 = rel.select(CoalesceOperator(ConstantExpression(42)))
        assert rel5.fetchone() == (42,)

        exprtest = con.table('exprtest')
        rel6 = exprtest.select(CoalesceOperator(ColumnExpression("a")))
        res = rel6.fetchall()
        assert res == [(42,), (43,), (None,), (45,)]

        rel7 = con.sql("select 42")
        rel7 = rel7.select(
            CoalesceOperator(
                ConstantExpression(None), ConstantExpression(None), ConstantExpression(42), ConstantExpression(43)
            )
        )
        res = rel7.fetchall()
        assert res == [(42,)]

        rel7 = con.sql("select 42")
        rel7 = rel7.select(CoalesceOperator(ConstantExpression(None), ConstantExpression(None), ConstantExpression(42)))
        res = rel7.fetchall()
        assert res == [(42,)]

        rel7 = con.sql("select 42")
        rel7 = rel7.select(CoalesceOperator(ConstantExpression(None), ConstantExpression(None), ConstantExpression(43)))
        res = rel7.fetchall()
        assert res == [(43,)]

        rel7 = con.sql("select 42")
        rel7 = rel7.select(
            CoalesceOperator(ConstantExpression(None), ConstantExpression(None), ConstantExpression(None))
        )
        res = rel7.fetchall()
        assert res == [(None,)]

        # These are converted tests
        # See 'test_coalesce.test_slow' for the original tests

        con.execute("SET default_null_order='nulls_first';")

        rel7 = exprtest.select(
            CoalesceOperator(
                ConstantExpression(None),
                ConstantExpression(None),
                ConstantExpression(None),
                ColumnExpression("a"),
                ConstantExpression(None),
                ColumnExpression("b"),
            )
        )
        res = rel7.fetchall()
        assert res == [(42,), (43,), (1,), (45,)]

        rel7 = exprtest.filter((ColumnExpression("b") == 1) | (CoalesceOperator("a", "b") == 42)).sort("a")
        res = rel7.fetchall()
        assert res == [(None, 1), (42, 10)]

        rel7 = exprtest.filter(
            (CoalesceOperator("a", "b") == 1) | (CoalesceOperator("a", "b") == 43) | (CoalesceOperator("a", "b") == 45)
        ).sort("a")
        res = rel7.fetchall()
        assert res == [(None, 1), (43, 100), (45, 0)]

        rel7 = exprtest.filter(
            (CoalesceOperator("a", "b") == 1)
            | (CoalesceOperator("a", "b") == 42)
            | (CoalesceOperator("a", "b") == 43)
            | (CoalesceOperator("a", "b") == 45)
        ).sort("a")
        res = rel7.fetchall()
        assert res == [(None, 1), (42, 10), (43, 100), (45, 0)]

        rel7 = exprtest.filter((ColumnExpression("b") == 1) | (CoalesceOperator("a", "b") == 1)).sort("a")
        res = rel7.fetchall()
        assert res == [(None, 1)]

    def test_column_expression_explain(self):
        con = duckdb.connect()

        rel = con.sql(
            """
            select 'unused'
        """
        )
        rel = rel.select(
            ConstantExpression("a").alias('c0'),
            ConstantExpression(42).alias('c1'),
            ConstantExpression(None).alias('c2'),
        )
        res = rel.explain()
        assert 'c0' in res
        assert 'c1' in res
        # 'c2' is not in the explain result because it shows NULL instead
        assert 'NULL' in res
        res = rel.fetchall()
        assert res == [('a', 42, None)]

    def test_column_expression_table(self):
        con = duckdb.connect()

        con.execute(
            """
            CREATE TABLE tbl as FROM (
                VALUES
                    ('a', 'b', 'c'),
                    ('d', 'e', 'f'),
                    ('g', 'h', 'i')
            ) t(c0, c1, c2)
        """
        )

        rel = con.table('tbl')
        rel2 = rel.select('c0', 'c1', 'c2')
        res = rel2.fetchall()
        assert res == [('a', 'b', 'c'), ('d', 'e', 'f'), ('g', 'h', 'i')]

    def test_column_expression_view(self):
        con = duckdb.connect()
        con.execute(
            """
            CREATE TABLE tbl as FROM (
                VALUES
                    ('a', 'b', 'c'),
                    ('d', 'e', 'f'),
                    ('g', 'h', 'i')
            ) t(c0, c1, c2)
        """
        )
        con.execute(
            """
            CREATE VIEW v1 as select c0 as c3, c2 as c4 from tbl;
        """
        )
        rel = con.view('v1')
        rel2 = rel.select('c3', 'c4')
        res = rel2.fetchall()
        assert res == [('a', 'c'), ('d', 'f'), ('g', 'i')]

    def test_column_expression_replacement_scan(self):
        con = duckdb.connect()

        pd = pytest.importorskip("pandas")
        df = pd.DataFrame({'a': [42, 43, 0], 'b': [True, False, True], 'c': [23.123, 623.213, 0.30234]})
        rel = con.sql("select * from df")
        rel2 = rel.select('a', 'b')
        res = rel2.fetchall()
        assert res == [(42, True), (43, False), (0, True)]

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
        rel2 = rel.select(expr)
        res = rel2.fetchall()
        assert res == [(2,)]

        res = rel.select(1 - col1).fetchall()
        assert res == [(-2,)]

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

    def test_between_expression(self):
        con = duckdb.connect()

        rel = con.sql(
            """
            select
                5 as a,
                2 as b,
                3 as c
        """
        )
        a = ColumnExpression('a')
        b = ColumnExpression('b')
        c = ColumnExpression('c')

        # 5 BETWEEN 2 AND 3 -> false
        assert rel.select(a.between(b, c)).fetchall() == [(False,)]

        # 2 BETWEEN 5 AND 3 -> false
        assert rel.select(b.between(a, c)).fetchall() == [(False,)]

        # 3 BETWEEN 5 AND 2 -> false
        assert rel.select(c.between(a, b)).fetchall() == [(False,)]

        # 3 BETWEEN 2 AND 5 -> true
        assert rel.select(c.between(b, a)).fetchall() == [(True,)]

    def test_collate_expression(self):
        con = duckdb.connect()
        rel = con.sql(
            """
            select
                'a' as c0,
                'A' as c1
            """
        )

        col1 = ColumnExpression('c0')
        col2 = ColumnExpression('c1')

        lower_a = ConstantExpression('a')
        upper_a = ConstantExpression('A')

        # SELECT c0 LIKE 'a' == True
        assert rel.select(FunctionExpression('~~', col1, lower_a)).fetchall() == [(True,)]

        # SELECT c0 LIKE 'A' == False
        assert rel.select(FunctionExpression('~~', col1, upper_a)).fetchall() == [(False,)]

        # SELECT c0 LIKE 'A' COLLATE NOCASE == True
        assert rel.select(FunctionExpression('~~', col1, upper_a.collate('NOCASE'))).fetchall() == [(True,)]

        # SELECT c1 LIKE 'a' == False
        assert rel.select(FunctionExpression('~~', col2, lower_a)).fetchall() == [(False,)]

        # SELECT c1 LIKE 'a' COLLATE NOCASE == True
        assert rel.select(FunctionExpression('~~', col2, lower_a.collate('NOCASE'))).fetchall() == [(True,)]

        with pytest.raises(duckdb.BinderException, match='collations are only supported for type varchar'):
            rel.select(FunctionExpression('~~', col2, lower_a).collate('NOCASE'))

        with pytest.raises(duckdb.CatalogException, match='Collation with name non-existant does not exist'):
            rel.select(FunctionExpression('~~', col2, lower_a.collate('non-existant')))

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

    def test_lambda_expression(self):
        con = duckdb.connect()

        rel = con.sql(
            """
            select
                [1,2,3] as a,
            """
        )

        # Use a tuple of strings as 'lhs'
        func = FunctionExpression(
            "list_reduce",
            ColumnExpression('a'),
            LambdaExpression(('x', 'y'), ColumnExpression('x') + ColumnExpression('y')),
        )
        rel2 = rel.select(func)
        res = rel2.fetchall()
        assert res == [(6,)]

        # Use only a string name as 'lhs'
        func = FunctionExpression("list_apply", ColumnExpression('a'), LambdaExpression('x', ColumnExpression('x') + 3))
        rel2 = rel.select(func)
        res = rel2.fetchall()
        assert res == [([4, 5, 6],)]

        # 'row' is not a lambda function, so it doesn't accept a lambda expression
        func = FunctionExpression("row", ColumnExpression('a'), LambdaExpression('x', ColumnExpression('x') + 3))
        with pytest.raises(duckdb.BinderException, match='This scalar function does not support lambdas'):
            rel2 = rel.select(func)

        # lhs has to be a tuple of strings or a single string
        with pytest.raises(
            ValueError, match="Please provide 'lhs' as either a tuple containing strings, or a single string"
        ):
            func = FunctionExpression(
                "list_filter", ColumnExpression('a'), LambdaExpression(42, ColumnExpression('x') + 3)
            )

        func = FunctionExpression(
            "list_filter", ColumnExpression('a'), LambdaExpression('x', ColumnExpression('y') != 3)
        )
        with pytest.raises(duckdb.BinderException, match='Referenced column "y" not found in FROM clause'):
            rel2 = rel.select(func)

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
        assert rel2.columns == ['b']

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

    def test_function_expression_udf(self):
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

    def test_function_expression_basic(self):
        con = duckdb.connect()

        rel = con.sql(
            """
                FROM (VALUES
                    (1, 'test', 3),
                    (2, 'this is a long string', 7),
                    (3, 'medium length', 4)
                ) tbl(text, start, "end")
            """
        )
        expr = FunctionExpression('array_slice', "start", "text", "end")
        rel2 = rel.select(expr)
        res = rel2.fetchall()
        assert res == [('tes',), ('his is',), ('di',)]

    def test_column_expression_function_coverage(self):
        con = duckdb.connect()

        con.execute(
            """
            CREATE TABLE tbl as FROM (
                VALUES
                    ('a', 'b', 'c'),
                    ('d', 'e', 'f'),
                    ('g', 'h', 'i')
            ) t(c0, c1, c2)
        """
        )

        rel = con.table('tbl')
        expr = FunctionExpression('||', FunctionExpression('||', 'c0', 'c1'), 'c2')
        rel2 = rel.select(expr)
        res = rel2.fetchall()
        assert res == [('abc',), ('def',), ('ghi',)]

    def test_function_expression_aggregate(self):
        con = duckdb.connect()

        rel = con.sql(
            """
                FROM (VALUES
                    ('test'),
                    ('this is a long string'),
                    ('medium length'),
                ) tbl(text)
            """
        )
        expr = FunctionExpression('first', 'text')
        with pytest.raises(
            duckdb.BinderException, match='Binder Error: Aggregates cannot be present in a Project relation!'
        ):
            rel2 = rel.select(expr)

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
        with pytest.raises(duckdb.OutOfRangeException, match="Overflow in multiplication of INT16"):
            expr = ColumnExpression("salary") * 100
            rel2 = rel.select(expr)
            res = rel2.fetchall()

        with pytest.raises(duckdb.OutOfRangeException, match="Overflow in multiplication of INT16"):
            val = duckdb.Value(100, duckdb.typing.TINYINT)
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

    def test_filter_equality(self, filter_rel):
        assert len(filter_rel.fetchall()) == 5

        expr = ColumnExpression("a") == 1
        rel2 = filter_rel.filter(expr)
        res = rel2.fetchall()
        assert len(res) == 2
        assert res == [(1, 'a'), (1, 'b')]

    def test_filter_not(self, filter_rel):
        expr = ColumnExpression("a") == 1
        # NOT operator
        expr = ~expr
        rel2 = filter_rel.filter(expr)
        res = rel2.fetchall()
        assert len(res) == 3
        assert res == [(2, 'b'), (3, 'c'), (4, 'a')]

    def test_filter_and(self, filter_rel):
        expr = ColumnExpression("a") == 1
        expr = ~expr
        # AND operator

        expr = expr & ('b' != ConstantExpression('b'))
        rel2 = filter_rel.filter(expr)
        res = rel2.fetchall()
        assert len(res) == 2
        assert res == [(3, 'c'), (4, 'a')]

    def test_filter_or(self, filter_rel):
        # OR operator
        expr = (ColumnExpression("a") == 1) | (ColumnExpression("a") == 4)
        rel2 = filter_rel.filter(expr)
        res = rel2.fetchall()
        assert len(res) == 3
        assert res == [(1, 'a'), (1, 'b'), (4, 'a')]

    def test_filter_mixed(self, filter_rel):
        # Mixed
        expr = (ColumnExpression("b") == ConstantExpression("a")) & (
            (ColumnExpression("a") == 1) | (ColumnExpression("a") == 4)
        )
        rel2 = filter_rel.filter(expr)
        res = rel2.fetchall()
        assert len(res) == 2
        assert res == [(1, 'a'), (4, 'a')]

    def test_empty_in(self, filter_rel):
        expr = ColumnExpression("a")
        with pytest.raises(
            duckdb.InvalidInputException, match="Incorrect amount of parameters to 'isin', needs at least 1 parameter"
        ):
            expr = expr.isin()

        expr = ColumnExpression("a")
        with pytest.raises(
            duckdb.InvalidInputException,
            match="Incorrect amount of parameters to 'isnotin', needs at least 1 parameter",
        ):
            expr = expr.isnotin()

    def test_filter_in(self, filter_rel):
        # IN expression
        expr = ColumnExpression("a")
        expr = expr.isin(1, 2)
        rel2 = filter_rel.filter(expr)
        res = rel2.fetchall()
        assert len(res) == 3
        assert res == [(1, 'a'), (1, 'b'), (2, 'b')]

    def test_filter_not_in(self, filter_rel):
        expr = ColumnExpression("a")
        expr = expr.isin(1, 2)
        # NOT IN expression
        expr = ~expr
        rel2 = filter_rel.filter(expr)
        res = rel2.fetchall()
        assert len(res) == 2
        assert res == [(3, 'c'), (4, 'a')]

        # NOT IN expression
        expr = ColumnExpression("a")
        expr = expr.isnotin(1, 2)
        rel2 = filter_rel.filter(expr)
        res = rel2.fetchall()
        assert len(res) == 2
        assert res == [(3, 'c'), (4, 'a')]

    def test_null(self):
        con = duckdb.connect()
        rel = con.sql(
            """
            select * from (VALUES
                (1, 'a'),
                (2, 'b'),
                (3, NULL),
                (4, 'c'),
                (5, 'a')
            ) tbl(a, b)
        """
        )

        b = ColumnExpression("b")

        res = rel.select(b.isnull()).fetchall()
        assert res == [(False,), (False,), (True,), (False,), (False,)]

        res2 = rel.filter(b.isnotnull()).fetchall()
        assert res2 == [(1, 'a'), (2, 'b'), (4, 'c'), (5, 'a')]

    def test_sort(self):
        con = duckdb.connect()
        rel = con.sql(
            """
            select * from (VALUES
                (1, 'a'),
                (2, 'b'),
                (3, NULL),
                (4, 'c'),
                (5, 'a')
            ) tbl(a, b)
        """
        )
        # Ascending sort order

        a = ColumnExpression("a")
        b = ColumnExpression("b")

        rel2 = rel.sort(a.asc())
        res = rel2.a.fetchall()
        assert res == [(1,), (2,), (3,), (4,), (5,)]

        # Descending sort order
        rel2 = rel.sort(a.desc())
        res = rel2.a.fetchall()
        assert res == [(5,), (4,), (3,), (2,), (1,)]

        # Nulls first
        rel2 = rel.sort(b.desc().nulls_first())
        res = rel2.b.fetchall()
        assert res == [(None,), ('c',), ('b',), ('a',), ('a',)]

        # Nulls last
        rel2 = rel.sort(b.desc().nulls_last())
        res = rel2.b.fetchall()
        assert res == [('c',), ('b',), ('a',), ('a',), (None,)]

    def test_aggregate(self):
        con = duckdb.connect()
        rel = con.sql("select * from range(1000000) t(a)")
        count = FunctionExpression("count", "a").cast("int")
        assert rel.aggregate([count]).execute().fetchone()[0] == 1000000
        assert rel.aggregate([count]).execute().fetchone()[0] == 1000000

    def test_aggregate_error(self):
        con = duckdb.connect()

        # Not necessarily an error, but even non-aggregates are accepted
        rel = con.sql("select * from values (5) t(a)")
        res = rel.aggregate(["a"]).execute().fetchone()[0]
        assert res == 5

        res = rel.aggregate([5]).execute().fetchone()[0]
        assert res == 5

        # Providing something that can not be converted into an expression is an error:
        with pytest.raises(
            duckdb.InvalidInputException, match='Invalid Input Error: Please provide arguments of type Expression!'
        ):

            class MyClass:
                def __init__(self):
                    pass

            res = rel.aggregate([MyClass()]).fetchone()[0]

        with pytest.raises(
            duckdb.InvalidInputException,
            match="Please provide either a string or list of Expression objects, not <class 'int'>",
        ):
            res = rel.aggregate(5).execute().fetchone()
