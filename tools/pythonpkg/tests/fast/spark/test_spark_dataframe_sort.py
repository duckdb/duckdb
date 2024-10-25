import pytest

_ = pytest.importorskip("duckdb.experimental.spark")

import spark_namespace.errors
from spark_namespace.sql.types import Row
from spark_namespace.errors import PySparkTypeError, PySparkValueError
from spark_namespace import USE_ACTUAL_SPARK


class TestDataFrameSort(object):
    data = [(56, "Carol"), (20, "Alice"), (3, "Dave"), (3, "Anna"), (1, "Ben")]

    def test_sort_ascending(self, spark):
        df = spark.createDataFrame(self.data, ["age", "name"])
        expected = [
            Row(age=1, name="Ben"),
            Row(age=3, name="Anna"),
            Row(age=3, name="Dave"),
            Row(age=20, name="Alice"),
            Row(age=56, name="Carol"),
        ]

        df = df.sort(["age", "name"])
        assert df.collect() == expected

        df = df.sort("age", "name")
        assert df.collect() == expected

        if not USE_ACTUAL_SPARK:
            # Spark does not support passing integers
            df = df.sort(1, 2)
            assert df.collect() == expected

    def test_sort_descending(self, spark):
        df = spark.createDataFrame(self.data, ["age", "name"])
        expected = [
            Row(age=20, name="Alice"),
            Row(age=3, name="Anna"),
            Row(age=1, name="Ben"),
            Row(age=56, name="Carol"),
            Row(age=3, name="Dave"),
        ]

        df = df.sort(["name", "age"])
        assert df.collect() == expected

        df = df.sort("name", "age")
        assert df.collect() == expected

        if not USE_ACTUAL_SPARK:
            # Spark does not support passing integers
            df = df.sort(2, 1)
            assert df.collect() == expected

    def test_sort_wrong_asc_params(self, spark):
        df = spark.createDataFrame(self.data, ["age", "name"])

        with pytest.raises(PySparkTypeError):
            df = df.sort(["age"], ascending="no")

    def test_sort_empty_params(self, spark):
        df = spark.createDataFrame(self.data, ["age", "name"])

        with pytest.raises(PySparkValueError):
            df = df.sort()

    # See https://github.com/apache/spark/commit/0193d0f88a953063c41c41042fb58bd0badc155c
    # for the PR which added that error to PySpark
    @pytest.mark.skipif(
        USE_ACTUAL_SPARK and not hasattr(spark_namespace.errors, "PySparkIndexError"),
        reason="PySparkIndexError is only introduced in PySpark 4.0.0",
    )
    def test_sort_zero_index(self, spark):
        df = spark.createDataFrame(self.data, ["age", "name"])

        from spark_namespace.errors import PySparkIndexError

        with pytest.raises(PySparkIndexError):
            df = df.sort(0)

    def test_sort_invalid_column(self, spark):
        df = spark.createDataFrame(self.data, ["age", "name"])

        with pytest.raises(PySparkTypeError):
            df = df.sort(dict(a=1))
