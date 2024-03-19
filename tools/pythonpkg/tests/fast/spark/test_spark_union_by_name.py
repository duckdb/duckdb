import pytest
from duckdb.experimental.spark.errors import PySparkValueError

from duckdb.experimental.spark.sql.types import Row

_ = pytest.importorskip("duckdb.experimental.spark")


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
class TestDataFrameUnion(object):
    def test_union_by_name(self, df1, df2):
        rel = df1.unionByName(df2).sort("name", "id")
        res = rel.collect()
        assert res == [
            Row(name="James", id=34),
            Row(name="James", id=34),
            Row(name="Jeff", id=34),
            Row(name="Jen", id=45),
            Row(name="Maria", id=24),
            Row(name="Maria", id=45),
            Row(name="Michael", id=56),
            Row(name="Robert", id=30),
        ]

    def test_union_by_name_different_columns(self, df1, df3):
        with pytest.raises(PySparkValueError):
            df1.unionByName(df3)

        with pytest.raises(NotImplementedError):
            df1.unionByName(df3, allowMissingColumns=False)
