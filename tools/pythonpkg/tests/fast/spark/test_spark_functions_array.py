import pytest

_ = pytest.importorskip("duckdb.experimental.spark")
from spark_namespace.sql import functions as F
from spark_namespace.sql.types import Row


class TestSparkFunctionsArray:
    def test_array_distinct(self, spark):
        data = [
            ([1, 2, 2], 2),
            ([2, 4, 5], 3),
        ]
        df = spark.createDataFrame(data, ["firstColumn", "secondColumn"])
        df = df.withColumn("distinct_values", F.array_distinct(F.col("firstColumn")))
        res = df.select("distinct_values").collect()
        # Output order can vary across platforms which is why we sort it first
        assert len(res) == 2
        assert sorted(res[0].distinct_values) == [1, 2]
        assert sorted(res[1].distinct_values) == [2, 4, 5]

    def test_array_intersect(self, spark):
        data = [
            (["b", "a", "c"], ["c", "d", "a", "f"]),
        ]
        df = spark.createDataFrame(data, ["c1", "c2"])
        df = df.withColumn("intersect_values", F.array_intersect(F.col("c1"), F.col("c2")))
        res = df.select("intersect_values").collect()
        # Output order can vary across platforms which is why we sort it first
        assert len(res) == 1
        assert sorted(res[0].intersect_values) == ["a", "c"]

    def test_array_union(self, spark):
        data = [
            (["b", "a", "c"], ["c", "d", "a", "f"]),
        ]
        df = spark.createDataFrame(data, ["c1", "c2"])
        df = df.withColumn("union_values", F.array_union(F.col("c1"), F.col("c2")))
        res = df.select("union_values").collect()
        # Output order can vary across platforms which is why we sort it first
        assert len(res) == 1
        assert sorted(res[0].union_values) == ["a", "b", "c", "d", "f"]

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

    def test_get(self, spark):
        df = spark.createDataFrame([(["a", "b", "c"], 1)], ['data', 'index'])

        res = df.select(F.get(df.data, 1).alias("r")).collect()
        assert res == [Row(r="b")]

        res = df.select(F.get(df.data, -1).alias("r")).collect()
        assert res == [Row(r=None)]

        res = df.select(F.get(df.data, 3).alias("r")).collect()
        assert res == [Row(r=None)]

        res = df.select(F.get(df.data, "index").alias("r")).collect()
        assert res == [Row(r='b')]

        res = df.select(F.get(df.data, F.col("index") - 1).alias("r")).collect()
        assert res == [Row(r='a')]

    def test_flatten(self, spark):
        df = spark.createDataFrame([([[1, 2, 3], [4, 5], [6]],), ([None, [4, 5]],)], ['data'])

        res = df.select(F.flatten(df.data).alias("r")).collect()
        assert res == [Row(r=[1, 2, 3, 4, 5, 6]), Row(r=None)]
