import pytest

_ = pytest.importorskip("duckdb.experimental.spark")
from spark_namespace.sql import functions as F
from spark_namespace.sql.types import Row


class TestsSparkFunctionsMiscellaneous:
    def test_call_function(self, spark):
        data = [
            (-1, 2),
            (4, 3),
        ]
        df = spark.createDataFrame(data, ["firstColumn", "secondColumn"])

        # Test with 2 columns as arguments
        df = df.withColumn("greatest_value", F.call_function("greatest", F.col("firstColumn"), F.col("secondColumn")))
        res = df.select("greatest_value").collect()
        assert res == [
            Row(greatest_value=2),
            Row(greatest_value=4),
        ]

        # Test with 1 column as argument
        df = df.withColumn("abs_value", F.call_function("abs", F.col("firstColumn")))
        res = df.select("abs_value").collect()
        assert res == [
            Row(abs_value=1),
            Row(abs_value=4),
        ]
