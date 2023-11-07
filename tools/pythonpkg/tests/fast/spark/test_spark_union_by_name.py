import pytest

_ = pytest.importorskip("duckdb.experimental.spark")

from duckdb.experimental.spark.sql.types import (
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
from duckdb.experimental.spark.sql.functions import col, struct, when, lit, array_contains
from duckdb.experimental.spark.sql.functions import sum, avg, max, min, mean, count


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


# https://sparkbyexamples.com/pyspark/pyspark-unionbyname/
@pytest.mark.skip(reason="union_by_name is not supported in the Relation API yet")
class TestDataFrameUnion(object):
    def test_union_by_name(self, df1, df2):
        rel = df1.unionByName(df2)
        res = rel.collect()
        print(res)
