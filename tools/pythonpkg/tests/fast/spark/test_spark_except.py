import platform
import pytest

_ = pytest.importorskip("duckdb.experimental.spark")

from duckdb.experimental.spark.sql.types import Row
from duckdb.experimental.spark.sql.functions import col


@pytest.fixture
def df(spark):
    return spark.createDataFrame([("a", 1), ("a", 1), ("a", 1), ("a", 2), ("b", 3), ("c", 4)], ["C1", "C2"])


@pytest.fixture
def df2(spark):
    return spark.createDataFrame([("a", 1), ("b", 3)], ["C1", "C2"])


class TestDataFrameIntersect:
    def test_exceptAll(self, spark, df, df2):

        df3 = df.exceptAll(df2).sort(*df.columns)
        res = df3.collect()

        assert res == [
            Row(C1="a", C2=1),
            Row(C1="a", C2=1),
            Row(C1="a", C2=2),
            Row(C1="c", C2=4),
        ]
