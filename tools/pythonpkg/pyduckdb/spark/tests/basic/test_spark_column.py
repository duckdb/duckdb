from pyduckdb.spark.sql.column import Column
from pyduckdb.spark.sql.functions import struct

import duckdb

class TestSparkColumn(object):
	def test_struct_column(self, spark):
		col = duckdb.ColumnExpression('a')
