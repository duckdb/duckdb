import pytest
from spark_namespace.sql import functions as F
from spark_namespace.sql.types import Row

_ = pytest.importorskip("duckdb.experimental.spark")


class TestSparkFunctionsExpr(object):
    def test_expr(self, spark):
        df = spark.createDataFrame([["Alice"], ["Bob"]], ["name"])
        res = df.select("name", F.expr("length(name)").alias("str_len")).collect()

        assert res == [
            Row(name="Alice", str_len=5),
            Row(name="Bob", str_len=3),
        ]
