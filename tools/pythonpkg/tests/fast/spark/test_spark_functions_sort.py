import pytest

_ = pytest.importorskip("duckdb.experimental.spark")

from spark_namespace.sql import functions as F
from spark_namespace.sql.types import Row


class TestSparkFunctionsSort:
    def test_asc(self, spark):
        df = spark.createDataFrame(
            [(1,), (3,), (2,)],
            ["a"],
        )

        res = df.orderBy(F.asc("a")).collect()
        assert res == [Row(a=1), Row(a=2), Row(a=3)]

        res = df.orderBy(F.asc(df["a"])).collect()
        assert res == [Row(a=1), Row(a=2), Row(a=3)]

    def test_desc(self, spark):
        df = spark.createDataFrame(
            [(1,), (3,), (2,)],
            ["a"],
        )

        res = df.orderBy(F.desc("a")).collect()
        assert res == [Row(a=3), Row(a=2), Row(a=1)]

        res = df.orderBy(F.desc(df["a"])).collect()
        assert res == [Row(a=3), Row(a=2), Row(a=1)]

    def test_asc_nulls_first(self, spark):
        df = spark.createDataFrame([(1, "Bob"), (0, None), (2, "Alice")], ["age", "name"])

        res = df.sort(F.asc_nulls_first(df["name"])).collect()
        assert res == [Row(age=0, name=None), Row(age=2, name="Alice"), Row(age=1, name="Bob")]

    def test_asc_nulls_last(self, spark):
        df = spark.createDataFrame([(1, "Bob"), (0, None), (2, "Alice")], ["age", "name"])

        res = df.sort(F.asc_nulls_last(df["name"])).collect()
        assert res == [Row(age=2, name="Alice"), Row(age=1, name="Bob"), Row(age=0, name=None)]

    def test_desc_nulls_first(self, spark):
        df = spark.createDataFrame([(1, "Bob"), (0, None), (2, "Alice")], ["age", "name"])

        res = df.sort(F.desc_nulls_first(df["name"])).collect()
        assert res == [Row(age=0, name=None), Row(age=1, name="Bob"), Row(age=2, name="Alice")]

    def test_desc_nulls_last(self, spark):
        df = spark.createDataFrame([(1, "Bob"), (0, None), (2, "Alice")], ["age", "name"])

        res = df.sort(F.desc_nulls_last(df["name"])).collect()
        assert res == [Row(age=1, name="Bob"), Row(age=2, name="Alice"), Row(age=0, name=None)]
