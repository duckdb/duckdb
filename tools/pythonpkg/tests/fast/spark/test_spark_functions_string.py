import pytest

_ = pytest.importorskip("duckdb.experimental.spark")
from duckdb.experimental.spark.sql import functions as F
from duckdb.experimental.spark.sql.types import Row


class TestSparkFunctionsString(object):
    def test_length(self, spark):
        data = [
            ("firstRowFirstColumn",),
            ("2ndRowFirstColumn",),
        ]
        df = spark.createDataFrame(data, ["firstColumn"])
        df = df.withColumn("length", F.length(F.col("firstColumn")))
        res = df.select("length").collect()
        assert res == [
            Row(length=19),
            Row(length=17),
        ]

    def test_upper(self, spark):
        data = [
            ("firstRowFirstColumn",),
            ("2ndRowFirstColumn",),
        ]
        df = spark.createDataFrame(data, ["firstColumn"])
        df = df.withColumn("upper", F.upper(F.col("firstColumn")))
        res = df.select("upper").collect()
        assert res == [
            Row(upper="FIRSTROWFIRSTCOLUMN"),
            Row(upper="2NDROWFIRSTCOLUMN"),
        ]

    def test_lower(self, spark):
        data = [
            ("firstRowFirstColumn",),
            ("2ndRowFirstColumn",),
        ]
        df = spark.createDataFrame(data, ["firstColumn"])
        df = df.withColumn("lower", F.lower(F.col("firstColumn")))
        res = df.select("lower").collect()
        assert res == [
            Row(lower="firstrowfirstcolumn"),
            Row(lower="2ndrowfirstcolumn"),
        ]

    def test_trim(self, spark):
        data = [
            (" firstRowFirstColumn ",),
            (" 2ndRowFirstColumn ",),
        ]
        df = spark.createDataFrame(data, ["firstColumn"])
        df = df.withColumn("trimmed", F.trim(F.col("firstColumn")))
        res = df.select("trimmed").collect()
        assert res == [
            Row(trimmed="firstRowFirstColumn"),
            Row(trimmed="2ndRowFirstColumn"),
        ]

    def test_ltrim(self, spark):
        data = [
            (" firstRowFirstColumn ",),
            (" 2ndRowFirstColumn ",),
        ]
        df = spark.createDataFrame(data, ["firstColumn"])
        df = df.withColumn("ltrimmed", F.ltrim(F.col("firstColumn")))
        res = df.select("ltrimmed").collect()
        assert res == [
            Row(ltrimmed="firstRowFirstColumn "),
            Row(ltrimmed="2ndRowFirstColumn "),
        ]

    def test_rtrim(self, spark):
        data = [
            (" firstRowFirstColumn ",),
            (" 2ndRowFirstColumn ",),
        ]
        df = spark.createDataFrame(data, ["firstColumn"])
        df = df.withColumn("rtrimmed", F.rtrim(F.col("firstColumn")))
        res = df.select("rtrimmed").collect()
        assert res == [
            Row(rtrimmed=" firstRowFirstColumn"),
            Row(rtrimmed=" 2ndRowFirstColumn"),
        ]
