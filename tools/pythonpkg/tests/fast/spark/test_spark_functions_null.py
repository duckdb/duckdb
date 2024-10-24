import pytest

_ = pytest.importorskip("duckdb.experimental.spark")
from spark_namespace.sql import functions as F
from spark_namespace.sql.types import Row


class TestsSparkFunctionsNull(object):
    def test_coalesce(self, spark):
        data = [
            (None, 2),
            (4, None),
        ]
        df = spark.createDataFrame(data, ["firstColumn", "secondColumn"])
        df = df.withColumn("coalesce_value", F.coalesce(F.col("firstColumn"), F.col("secondColumn")))
        res = df.select("coalesce_value").collect()
        assert res == [
            Row(coalesce_value=2),
            Row(coalesce_value=4),
        ]

    def test_nvl(self, spark):
        data = [
            (None, 2),
            (4, None),
        ]
        df = spark.createDataFrame(data, ["firstColumn", "secondColumn"])
        df = df.withColumn("nvl_value", F.nvl(F.col("firstColumn"), F.col("secondColumn")))
        res = df.select("nvl_value").collect()
        assert res == [
            Row(nvl_value=2),
            Row(nvl_value=4),
        ]

    def test_ifnull(self, spark):
        data = [
            (None, 2),
            (4, None),
        ]
        df = spark.createDataFrame(data, ["firstColumn", "secondColumn"])
        df = df.withColumn("ifnull_value", F.ifnull(F.col("firstColumn"), F.col("secondColumn")))
        res = df.select("ifnull_value").collect()
        assert res == [
            Row(nvl_value=2),
            Row(nvl_value=4),
        ]
