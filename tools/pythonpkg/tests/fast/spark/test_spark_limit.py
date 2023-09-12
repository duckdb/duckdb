import pytest

_ = pytest.importorskip("pyduckdb.spark")

from pyduckdb.spark.sql.types import (
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

    # PySpark's 'limit' does not support providing an offset, this is an extension to the Spark API
    def test_dataframe_offset(self, spark):
        df = spark.sql("select * from range(100000)")
        df2 = df.limit(10, 99950)
        res = df2.collect()
        expected = [
            Row(range=99950),
            Row(range=99951),
            Row(range=99952),
            Row(range=99953),
            Row(range=99954),
            Row(range=99955),
            Row(range=99956),
            Row(range=99957),
            Row(range=99958),
            Row(range=99959),
        ]
        assert res == expected
