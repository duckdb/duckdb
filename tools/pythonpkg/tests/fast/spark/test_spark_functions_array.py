import pytest

_ = pytest.importorskip("duckdb.experimental.spark")
from duckdb.experimental.spark.sql import functions as F
from duckdb.experimental.spark.sql.types import Row

class TestSparkFunctionsArray:
    def test_array_distinct(self, spark):
        data = [
            ([1, 2, 2], 2),
            ([2, 4, 5], 3),
        ]
        df = spark.createDataFrame(data, ["firstColumn", "secondColumn"])
        df = df.withColumn("distinct_values", F.array_distinct(F.col("firstColumn")))
        res = df.select("distinct_values").collect()
        assert res == [
            Row(distinct_values=[2, 1]),
            Row(distinct_values=[5, 4, 2]),
        ]

    def test_array_intersect(self, spark):
        data = [
            (["b", "a", "c"], ["c", "d", "a", "f"]),
        ]
        df = spark.createDataFrame(data, ["c1", "c2"])
        df = df.withColumn("intersect_values", F.array_intersect(F.col("c1"), F.col("c2")))
        res = df.select("intersect_values").collect()
        assert res ==  [
            Row(intersect_values=["c", "a"]),
        ]