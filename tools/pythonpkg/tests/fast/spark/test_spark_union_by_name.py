import pytest
from duckdb.experimental.spark.errors import IllegalArgumentException

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


@pytest.fixture
def df3(spark):
    data2 = [
        (1,),
    ]
    dataframe = spark.createDataFrame(data=data2, schema=["extra_col"])
    yield dataframe


class TestDataFrameUnion(object):
    def test_union_by_name(self, df1, df2):
        rel = df1.unionByName(df2)

        assert rel.columns == ["name", "id"]
        assert rel.count() == 8

        assert rel.select("name").distinct().count() == 6
        assert rel.select("id").distinct().count() == 5

    def test_error_union_by_name(self, df1, df3):
        with pytest.raises(IllegalArgumentException):
            df1.unionByName(df3, allowMissingColumns=False)

        with pytest.raises(IllegalArgumentException):
            df1.unionByName(df3, allowMissingColumns=False)

    def test_union_by_name_allow_missing_columns(self, df1, df3):
        rel = df1.unionByName(df3, allowMissingColumns=True)

        assert rel.columns == ["name", "id", "extra_col"]
        assert rel.count() == 5
