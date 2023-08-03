import pytest
from pyduckdb.spark.sql import SparkSession


# By making the scope 'function' we ensure that a new connection gets created for every function that uses the fixture
@pytest.fixture(scope='function', autouse=True)
def spark():
    return SparkSession.builder.master(':memory:').appName('pyspark').getOrCreate()
