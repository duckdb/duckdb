import duckdb
import pytest
from duckdb.typing import (
	INTEGER
)
from duckdb import (
	Expression,
	BinaryFunctionExpression,
	ConstantExpression,
	ColumnExpression
)
from pyduckdb.value.constant import Value

class TestExpression(object):
	def test_constant_expression(self):
		con = duckdb.connect()

		val = Value(5, INTEGER)

		rel = con.sql("""
			select
				1 as a,
				2 as b,
				3 as c
		""")

		constant = ConstantExpression(val)

		rel = rel.select(constant)
		res = rel.fetchall()
		assert res == [(5,)]

	def test_column_expression(self):
		con = duckdb.connect()

		rel = con.sql("""
			select
				1 as a,
				2 as b,
				3 as c
		""")
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

		rel = con.sql("""
			select
				1 as a,
				2 as b,
				3 as c
		""")

		constant = ConstantExpression(val)
		col = ColumnExpression('b')
		expr = col + constant

		rel = rel.select(expr, expr)
		res = rel.fetchall()
		assert res == [(7,7)]

	def test_binary_function_expression(self):
		con = duckdb.connect()

		rel = con.sql("""
			select
				1 as a,
				5 as b
		""")
		function = BinaryFunctionExpression("-", ColumnExpression('b'), ColumnExpression('a'))
		rel2 = rel.select(function)
		res = rel2.fetchall()
		assert res == [(4,)]

	def test_negate_expression(self):
		con = duckdb.connect()

		rel = con.sql("""
			select 5 as a
		""")
		col = ColumnExpression('a')
		col = -col;
		rel = rel.select(col)
		res = rel.fetchall()
		assert res == [(-5,)]
	
	def test_subtract_expression(self):
		con = duckdb.connect()

		rel = con.sql("""
			select
				3 as a,
				1 as b
		""")
		col1 = ColumnExpression('a')
		col2 = ColumnExpression('b')
		expr = col1 - col2
		rel = rel.select(expr)
		res = rel.fetchall()
		assert res == [(2,)]

	def test_multiply_expression(self):
		con = duckdb.connect()

		rel = con.sql("""
			select
				3 as a,
				2 as b
		""")
		col1 = ColumnExpression('a')
		col2 = ColumnExpression('b')
		expr = col1 * col2
		rel = rel.select(expr)
		res = rel.fetchall()
		assert res == [(6,)]

	def test_division_expression(self):
		con = duckdb.connect()

		rel = con.sql("""
			select
				5 as a,
				2 as b
		""")
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

		rel = con.sql("""
			select
				5 as a,
				2 as b
		""")
		col1 = ColumnExpression('a')
		col2 = ColumnExpression('b')
		expr = col1 % col2
		rel2 = rel.select(expr)
		res = rel2.fetchall()
		assert res == [(1,)]

	def test_power_expression(self):
		con = duckdb.connect()

		rel = con.sql("""
			select
				5 as a,
				2 as b
		""")
		col1 = ColumnExpression('a')
		col2 = ColumnExpression('b')
		expr = col1 ** col2
		rel2 = rel.select(expr)
		res = rel2.fetchall()
		assert res == [(25,)]

	def test_equality_expression(self):
		con = duckdb.connect()

		rel = con.sql("""
			select
				5 as a,
				2 as b,
				5 as c
		""")
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

		rel = con.sql("""
			select
				5 as a,
				2 as b,
				5 as c
		""")
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

		rel = con.sql("""
			select
				1 as a,
				2 as b,
				3 as c,
				3 as d
		""")
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
