import pytest

_ = pytest.importorskip("duckdb.experimental.spark")
from duckdb.experimental.spark.sql.types import Row
from duckdb.experimental.spark.sql.functions import concat_ws, col


class TestReplaceEmpty(object):
    def test_replace_empty(self, spark):
        data = [
            ("firstRowFirstColumn", "firstRowSecondColumn"),
            ("2ndRowFirstColumn", "2ndRowSecondColumn"),
        ]
        df = spark.createDataFrame(data, ["firstColumn", "secondColumn"])
        df = df.withColumn("concatted", concat_ws(" ", col("firstColumn"), col("secondColumn")))
        res = df.select("concatted").collect()
        assert res == [
            Row(concatted="firstRowFirstColumn firstRowSecondColumn"),
            Row(concatted="2ndRowFirstColumn 2ndRowSecondColumn"),
        ]
