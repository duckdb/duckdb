import pytest

class TestDataFrame(object):
	@pytest.mark.skip("can't create a dataframe from a list of tuples yet")
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
		spark.sql("""
			create table sample_table("_1" bool, "_2" integer)
		""")
		spark.table("sample_table").write.saveAsTable("sample_hive_table")
		df3 = spark.sql("SELECT _1,_2 FROM sample_hive_table")
		df3.show()
