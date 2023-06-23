

import pytest

class TestSparkConfig(object):
	def test_spark_config(self, spark):
		# Set Config
		spark.conf.set("spark.executor.memory", "5g")

		# Get a Spark Config
		partitions = spark.conf.get("spark.sql.shuffle.partitions")
		print(partitions)
