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


class TestDataFrameUnion(object):
    def test_union_by_name(self, df1, df2):
        rel = df1.unionByName(df2)
        res = rel.collect()
        expected = [
            Row(name='James', id=34),
            Row(name='Michael', id=56),
            Row(name='Robert', id=30),
            Row(name='Maria', id=24),
            Row(name='James', id=34),
            Row(name='Maria', id=45),
            Row(name='Jen', id=45),
            Row(name='Jeff', id=34),
        ]
        assert res == expected

    def test_union_by_name_allow_missing_cols(self, df1, df2):
        rel = df1.unionByName(df2.drop("id"), allowMissingColumns=True)
        res = rel.collect()
        expected = [
            Row(name='James', id=34),
            Row(name='Michael', id=56),
            Row(name='Robert', id=30),
            Row(name='Maria', id=24),
            Row(name='James', id=None),
            Row(name='Maria', id=None),
            Row(name='Jen', id=None),
            Row(name='Jeff', id=None),
        ]
        assert res == expected
