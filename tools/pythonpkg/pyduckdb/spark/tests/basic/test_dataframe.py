import pytest
from pyduckdb.spark.sql import SparkSession

@pytest.fixture(scope='session', autouse=True)
def spark():
	return SparkSession.builder.master(':memory:').appName('pyspark').getOrCreate()

class TestDataFrame(object):
	def test_dataframe(self, spark):
		# Create DataFrame
		df = spark.createDataFrame(
			[("Scala", 25000), ("Spark", 35000), ("PHP", 21000)])
		df.show()

		# Output
		#+-----+-----+
		#|   _1|   _2|
		#+-----+-----+
		#|Scala|25000|
		#|Spark|35000|
		#|  PHP|21000|
		#+-----+-----+

	def test_writing_to_table(self, spark):
		# Create Hive table & query it.
		spark.table("sample_table").write.saveAsTable("sample_hive_table")
		df3 = spark.sql("SELECT _1,_2 FROM sample_hive_table")
		df3.show()
