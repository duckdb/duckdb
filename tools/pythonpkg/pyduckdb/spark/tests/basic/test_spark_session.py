import pytest
import pyduckdb.spark as pyspark
from pyduckdb.spark.sql import SparkSession

@pytest.fixture(scope='session', autouse=True)
def basic_session():
	return SparkSession.builder.master(':memory:').appName('pyspark').getOrCreate()

class TestSparkSession(object):
	def test_spark_session(self):
		spark = SparkSession.builder.master("local[1]") \
							.appName('SparkByExamples.com') \
							.getOrCreate()
	def test_new_session(self, basic_session: SparkSession):
		spark = basic_session.newSession()
		print(spark)

	@pytest.mark.skip(reason='not tested yet')
	def test_retrieve_same_session(self):
		spark = SparkSession.builder.master('test').appName('test2').getOrCreate()
		spark2 = SparkSession.builder.getOrCreate()
		# Same connection should be returned
		assert spark == spark2

	def test_config(self):
		# Usage of config()
		spark = SparkSession.builder \
			.master("local[1]") \
			.appName("SparkByExamples.com") \
			.config("spark.some.config.option", "config-value") \
			.getOrCreate()

	def test_hive_support(self):
		# Enabling Hive to use in Spark
		spark = SparkSession.builder \
			.master("local[1]") \
			.appName("SparkByExamples.com") \
			.config("spark.sql.warehouse.dir", "<path>/spark-warehouse") \
			.enableHiveSupport() \
			.getOrCreate()
