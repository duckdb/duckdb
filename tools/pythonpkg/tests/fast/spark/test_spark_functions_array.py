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

    def test_array_union(self, spark):
        data = [
            (["b", "a", "c"], ["c", "d", "a", "f"]),
        ]
        df = spark.createDataFrame(data, ["c1", "c2"])
        df = df.withColumn("union_values", F.array_union(F.col("c1"), F.col("c2")))
        res = df.select("union_values").collect()
        assert res == [
            Row(union_values=["f", "d", "c", "a", "b"]),
        ]

    def test_array_max(self, spark):
        data = [
            ([1, 2, 3], 3),
            ([4, 2, 5], 5),
        ]
        df = spark.createDataFrame(data, ["firstColumn", "secondColumn"])
        df = df.withColumn("max_value", F.array_max(F.col("firstColumn")))
        res = df.select("max_value").collect()
        assert res == [
            Row(max_value=3),
            Row(max_value=5),
        ]


    def test_array_min(self, spark):
        data = [
            ([2, 1, 3], 3),
            ([2, 4, 5], 5),
        ]
        df = spark.createDataFrame(data, ["firstColumn", "secondColumn"])
        df = df.withColumn("min_value", F.array_min(F.col("firstColumn")))
        res = df.select("min_value").collect()
        assert res == [
            Row(max_value=1),
            Row(max_value=2),
        ]