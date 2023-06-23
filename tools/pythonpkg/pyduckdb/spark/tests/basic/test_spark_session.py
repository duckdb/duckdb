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
