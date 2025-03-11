import pytest

_ = pytest.importorskip("duckdb.experimental.spark")

from spark_namespace import USE_ACTUAL_SPARK
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

    @pytest.mark.skipif(
        USE_ACTUAL_SPARK and not hasattr(F, "zeroifnull"),
        reason="zeroifnull is only introduced in PySpark 4.0.0",
    )
    def test_zeroifnull(self, spark):
        df = spark.createDataFrame([(None,), (1,)], ["a"])
        res = df.select(F.zeroifnull(df.a).alias("result")).collect()
        assert res == [
            Row(result=0),
            Row(result=1),
        ]

    def test_nvl2(self, spark):
        df = spark.createDataFrame(
            [
                (
                    None,
                    8,
                    6,
                ),
                (
                    1,
                    9,
                    9,
                ),
            ],
            ["a", "b", "c"],
        )
        res = df.select(F.nvl2(df.a, df.b, df.c).alias('r')).collect()
        assert res == [Row(r=6), Row(r=9)]

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

    def test_nullif(self, spark):
        df = spark.createDataFrame(
            [
                (
                    None,
                    None,
                ),
                (
                    1,
                    9,
                ),
            ],
            ["a", "b"],
        )
        res = df.select(F.nullif(df.a, df.b).alias('r')).collect()
        assert res == [Row(r=None), Row(r=1)]

    def test_isnull(self, spark):
        df = spark.createDataFrame([(1, None), (None, 2)], ("a", "b"))

        res = df.select("a", "b", F.isnull("a").alias("r1"), F.isnull(df.b).alias("r2")).collect()
        assert res == [
            Row(a=1, b=None, r1=False, r2=True),
            Row(a=None, b=2, r1=True, r2=False),
        ]

    def test_isnotnull(self, spark):
        df = spark.createDataFrame([(1, None), (None, 2)], ("a", "b"))

        res = df.select("a", "b", F.isnotnull("a").alias("r1"), F.isnotnull(df.b).alias("r2")).collect()
        assert res == [
            Row(a=1, b=None, r1=True, r2=False),
            Row(a=None, b=2, r1=False, r2=True),
        ]
