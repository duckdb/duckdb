import pytest

_ = pytest.importorskip("duckdb.experimental.spark")
from duckdb.experimental.spark.sql import functions as F


class TestSparkFunctionsHash(object):
    def test_md5(self, spark):
        data = [
            ("quack",),
        ]
        res = (
            spark.createDataFrame(data, ["firstColumn"])
            .withColumn("hashed_value", F.md5(F.col("firstColumn")))
            .select("hashed_value")
            .collect()
        )
        assert res[0].hashed_value == "cfaf278e8f522c72644cee2a753d2845"

    def test_sha256(self, spark):
        data = [
            ("quack",),
        ]
        res = (
            spark.createDataFrame(data, ["firstColumn"])
            .withColumn("hashed_value", F.sha2(F.col("firstColumn"), 256))
            .select("hashed_value")
            .collect()
        )
        assert res[0].hashed_value == "82d928273d067d774889d5df4249aaf73c0b04c64f04d6ed001441ce87a0853c"
