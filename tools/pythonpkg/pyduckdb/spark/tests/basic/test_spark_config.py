

import pytest
from pyduckdb.spark.sql import SparkSession

@pytest.fixture(scope='session', autouse=True)
def spark():
	return SparkSession.builder.master(':memory:').appName('pyspark').getOrCreate()

class TestSparkConfig(object):
	def test_spark_config(self, spark):
		# Set Config
		spark.conf.set("spark.executor.memory", "5g")

		# Get a Spark Config
		partitions = spark.conf.get("spark.sql.shuffle.partitions")
		print(partitions)
