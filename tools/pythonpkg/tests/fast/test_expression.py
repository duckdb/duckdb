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
