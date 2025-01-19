import pytest

_ = pytest.importorskip("duckdb.experimental.spark")


from spark_namespace.sql.types import (
    LongType,
    StructType,
    BooleanType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    Row,
    ArrayType,
    MapType,
)
from spark_namespace.sql.functions import col, struct, when, lit, array_contains
from spark_namespace.sql.functions import sum, avg, max, min, mean, count


@pytest.fixture
def df1(spark):
    data = [("James", 34), ("Michael", 56), ("Robert", 30), ("Maria", 24)]
    dataframe = spark.createDataFrame(data=data, schema=["name", "id"])
    yield dataframe


@pytest.fixture
def df2(spark):
    data2 = [(34, "James"), (45, "Maria"), (45, "Jen"), (34, "Jeff")]
    dataframe = spark.createDataFrame(data=data2, schema=["id", "name"])
    yield dataframe


class TestDataFrameCache(object):
    def test_cache_two_tables(self, df1, df2):
        cached_one_df = df1.cache()
        result_one = cached_one_df.collect()
        expected_one = [
            Row(name='James', id=34),
            Row(name='Michael', id=56),
            Row(name='Robert', id=30),
            Row(name='Maria', id=24),
        ]
        assert result_one == expected_one
        cached_two_df = df2.cache()
        result_two = cached_two_df.collect()
        expected_two = [
            Row(id=34, name='James'),
            Row(id=45, name='Maria'),
            Row(id=45, name='Jen'),
            Row(id=34, name='Jeff'),
        ]
        assert result_two == expected_two
