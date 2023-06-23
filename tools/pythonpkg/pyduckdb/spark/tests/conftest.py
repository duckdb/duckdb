import pytest
from pyduckdb.spark.sql import SparkSession

@pytest.fixture(scope='session', autouse=True)
def spark():
	return SparkSession.builder.master(':memory:').appName('pyspark').getOrCreate()
