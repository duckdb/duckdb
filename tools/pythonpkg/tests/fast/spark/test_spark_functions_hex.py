import pytest
import sys

_ = pytest.importorskip("duckdb.experimental.spark")
from spark_namespace.sql import functions as F


class TestSparkFunctionsHex(object):
    def test_hex_string_col(self, spark):
        data = [
            ("quack",),
        ]
        res = (
            spark.createDataFrame(data, ["firstColumn"])
            .withColumn("hex_value", F.hex(F.col("firstColumn")))
            .select("hex_value")
            .collect()
        )
        assert res[0].hex_value == "717561636B"

    def test_hex_binary_col(self, spark):
        data = [
            (b'quack',),
        ]
        res = (
            spark.createDataFrame(data, ["firstColumn"])
            .withColumn("hex_value", F.hex(F.col("firstColumn")))
            .select("hex_value")
            .collect()
        )
        assert res[0].hex_value == "717561636B"

    def test_hex_integer_col(self, spark):
        data = [
            (int(42),),
        ]
        res = (
            spark.createDataFrame(data, ["firstColumn"])
            .withColumn("hex_value", F.hex(F.col("firstColumn")))
            .select("hex_value")
            .collect()
        )
        assert res[0].hex_value == "2A"

    # def test_hex_long_col(self, spark):
    #     long_value = sys.maxsize + 1
    #     data = [
    #         (long_value,),
    #     ]
    #     res = (
    #         spark.createDataFrame(data, ["firstColumn"])
    #         .withColumn("hex_value", F.hex(F.col("firstColumn")))
    #         .select("hex_value")
    #         .collect()
    #     )
    #     assert res[0].hex_value == hex(long_value)[2:]

    def test_unhex(self, spark):
        data = [
            ("717561636B",),
        ]
        res = (
            spark.createDataFrame(data, ["firstColumn"])
            .withColumn("unhex_value", F.unhex(F.col("firstColumn")))
            .select("unhex_value")
            .collect()
        )
        assert res[0].unhex_value == b'quack'
