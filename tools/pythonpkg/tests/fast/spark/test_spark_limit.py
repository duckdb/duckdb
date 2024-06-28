import pytest

_ = pytest.importorskip("duckdb.experimental.spark")

from duckdb.experimental.spark.sql.types import (
    Row,
)


class TestDataFrameLimit(object):
    def test_dataframe_limit(self, spark):
        df = spark.sql("select * from range(100000)")
        df2 = df.limit(10)
        res = df2.collect()
        expected = [
            Row(range=0),
            Row(range=1),
            Row(range=2),
            Row(range=3),
            Row(range=4),
            Row(range=5),
            Row(range=6),
            Row(range=7),
            Row(range=8),
            Row(range=9),
        ]
        assert res == expected
