import pytest
import pyduckdb.spark as pyspark
from pyduckdb.spark.sql import SparkSession

class TestSparkSession(object):
	def test_spark_session(self):
		spark = SparkSession.builder.master("local[1]") \
							.appName('SparkByExamples.com') \
							.getOrCreate()
