import pytest

_ = pytest.importorskip("duckdb.experimental.spark")

from spark_namespace import USE_ACTUAL_SPARK
from spark_namespace.sql import functions as F
from spark_namespace.sql.types import Row


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

    def test_endswith(self, spark):
        data = [
            ("firstRowFirstColumn", "Column"),
            ("2ndRowFirstColumn", "column"),
        ]
        df = spark.createDataFrame(data, ["firstColumn", "secondColumn"])
        df = df.withColumn("endswith", F.endswith(F.col("firstColumn"), F.col("secondColumn")))
        res = df.select("endswith").collect()
        assert res == [
            Row(endswith=True),
            Row(endswith=False),
        ]

    def test_startswith(self, spark):
        data = [
            ("firstRowFirstColumn", "irst"),
            ("2ndRowFirstColumn", "2nd"),
        ]
        df = spark.createDataFrame(data, ["firstColumn", "secondColumn"])
        df = df.withColumn("startswith", F.startswith(F.col("firstColumn"), F.col("secondColumn")))
        res = df.select("startswith").collect()
        assert res == [
            Row(startswith=False),
            Row(startswith=True),
        ]

    def test_ascii(self, spark):
        df = spark.createDataFrame([("Spark",), ("PySpark",), ("Pandas API",)], ["value"])

        res = df.select(F.ascii("value").alias("a")).collect()
        assert res == [Row(a=83), Row(a=80), Row(a=80)]

    def test_btrim(self, spark):
        df = spark.createDataFrame(
            [
                (
                    "SSparkSQLS",
                    "SL",
                )
            ],
            ['a', 'b'],
        )

        res = df.select(F.btrim(df.a, df.b).alias('r')).collect()
        assert res == [Row(r='parkSQ')]

        df = spark.createDataFrame([("    SparkSQL   ",)], ['a'])
        res = df.select(F.btrim(df.a).alias('r')).collect()
        assert res == [Row(r='SparkSQL')]

    def test_char(self, spark):
        df = spark.createDataFrame(
            [(65,), (65 + 256,), (66 + 256,)],
            [
                'a',
            ],
        )

        res = df.select(F.char(df.a).alias('ch')).collect()
        assert res == [Row(ch='A'), Row(ch='A'), Row(ch='B')]

    def test_encode(self, spark):
        df = spark.createDataFrame([('abcd',)], ['c'])

        res = df.select(F.encode("c", "UTF-8").alias("encoded")).collect()
        # FIXME: Should return the same type
        if USE_ACTUAL_SPARK:
            assert res == [Row(encoded=bytearray(b'abcd'))]
        else:
            assert res == [Row(encoded=b'abcd')]

    def test_find_in_set(self, spark):
        string_array = "abc,b,ab,c,def"
        df = spark.createDataFrame([("ab", string_array), ("b,c", string_array), ("z", string_array)], ['a', 'b'])

        res = df.select(F.find_in_set(df.a, df.b).alias('r')).collect()

        assert res == [Row(r=3), Row(r=0), Row(r=0)]
