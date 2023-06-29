import pytest
from pyduckdb.spark.sql.types import Row
from pyduckdb.spark.sql.types import LongType, StructType, BooleanType, StructField

class TestDataFrame(object):
	def test_dataframe(self, spark):
		# Create DataFrame
		df = spark.createDataFrame(
			[("Scala", 25000), ("Spark", 35000), ("PHP", 21000)])
		res = df.collect()
		assert res == [
			Row(col0='Scala', col1=25000),
			Row(col0='Spark', col1=35000),
			Row(col0='PHP', col1=21000)
		]

	def test_writing_to_table(self, spark):
		# Create Hive table & query it.
		spark.sql("""
			create table sample_table("_1" bool, "_2" integer)
		""")
		spark.sql('insert into sample_table VALUES (True, 42)')
		spark.table("sample_table").write.saveAsTable("sample_hive_table")
		df3 = spark.sql("SELECT _1,_2 FROM sample_hive_table")
		res = df3.collect()
		assert res == [Row(_1=True, _2=42)]

	def test_dataframe_collect(self, spark):
		df = spark.createDataFrame([(42,), (21,)]).toDF('a')
		res = df.collect()
		assert str(res) == '[Row(a=42), Row(a=21)]'

	def test_dataframe_from_rows(self, spark):
		columns = ["language","users_count"]
		data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]

		rowData = map(lambda x: Row(*x), data)
		df = spark.createDataFrame(rowData,columns)
		res = df.collect()
		assert res == [
			Row(language='Java', users_count='20000'),
			Row(language='Python', users_count='100000'),
			Row(language='Scala', users_count='3000')
		]

	def test_df_from_pandas(self, spark):
		import pandas as pd
		df = spark.createDataFrame(pd.DataFrame({'a': [42, 21], 'b': [True, False]}))
		res = df.collect()
		assert res == [Row(a=42, b=True), Row(a=21, b=False)]

	def test_df_from_struct_type(self, spark):
		schema = StructType([
			StructField('a', LongType()),
			StructField('b', BooleanType())
		])
		df = spark.createDataFrame([(42,True), (21,False)], schema)
		res = df.collect()
		assert res == [Row(a=42, b=True), Row(a=21, b=False)]

	def test_df_from_name_list(self, spark):
		df = spark.createDataFrame([(42,True), (21,False)], ['a', 'b'])
		res = df.collect()
		assert res == [Row(a=42, b=True), Row(a=21, b=False)]
