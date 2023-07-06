from pyduckdb.spark.sql.column import Column
from pyduckdb.spark.sql.functions import struct
from pyduckdb.spark.sql.types import Row

import duckdb

class TestSparkColumn(object):
	def test_struct_column(self, spark):
		df = spark.createDataFrame([Row(a=1, b=2, c=3, d=4)])
		df.show()

		df = df.withColumn('struct', struct(df.col0, df.col1))
		df.show()
		# TODO: assert that the resulting dataframe contains 'struct' column
