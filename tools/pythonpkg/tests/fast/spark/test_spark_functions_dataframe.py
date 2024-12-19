import pytest

_ = pytest.importorskip("duckdb.experimental.spark")
from spark_namespace.sql import functions as F


class TestSparkFunctionsArray:
    def test_broadcast(self, spark):
        data = [
            ([1, 2, 2], 2),
            ([2, 4, 5], 3),
        ]

        df = spark.createDataFrame(data, ["firstColumn", "secondColumn"])
        df_broadcast = F.broadcast(df)

        assert df.collect() == df_broadcast.collect()
