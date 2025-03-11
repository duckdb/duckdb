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

    def test_ucase(self, spark):
        data = [
            ("firstRowFirstColumn",),
            ("2ndRowFirstColumn",),
        ]
        df = spark.createDataFrame(data, ["firstColumn"])
        df = df.withColumn("upper", F.ucase(F.col("firstColumn")))
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

    def test_lcase(self, spark):
        data = [
            ("firstRowFirstColumn",),
            ("2ndRowFirstColumn",),
        ]
        df = spark.createDataFrame(data, ["firstColumn"])
        df = df.withColumn("lower", F.lcase(F.col("firstColumn")))
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

    def test_split(self, spark):
        df = spark.createDataFrame(
            [('oneAtwoBthreeC',)],
            [
                's',
            ],
        )

        res = df.select(F.split(df.s, '[ABC]').alias('s')).collect()
        assert res == [Row(s=['one', 'two', 'three', ''])]

    def test_split_part(self, spark):
        df = spark.createDataFrame(
            [
                (
                    "11.12.13",
                    ".",
                    3,
                )
            ],
            ["a", "b", "c"],
        )

        res = df.select(F.split_part(df.a, df.b, df.c).alias('r')).collect()
        assert res == [Row(r='13')]

        # If any input is null, should return null
        df = spark.createDataFrame(
            [
                (
                    "11.12.13",
                    ".",
                    None,
                ),
                (
                    "11.12.13",
                    ".",
                    1,
                ),
            ],
            ["a", "b", "c"],
        )
        res = df.select(F.split_part(df.a, df.b, df.c).alias('r')).collect()
        assert res == [Row(r=None), Row(r='11')]

        # If partNum is out of range, should return an empty string
        df = spark.createDataFrame(
            [
                (
                    "11.12.13",
                    ".",
                    4,
                )
            ],
            ["a", "b", "c"],
        )
        res = df.select(F.split_part(df.a, df.b, df.c).alias('r')).collect()
        assert res == [Row(r='')]

        # If partNum is negative, parts are counted backwards
        df = spark.createDataFrame(
            [
                (
                    "11.12.13",
                    ".",
                    -1,
                )
            ],
            ["a", "b", "c"],
        )
        res = df.select(F.split_part(df.a, df.b, df.c).alias('r')).collect()
        assert res == [Row(r='13')]

        # If the delimiter is an empty string, the return should be empty
        df = spark.createDataFrame(
            [
                (
                    "11.12.13",
                    "",
                    2,
                )
            ],
            ["a", "b", "c"],
        )
        res = df.select(F.split_part(df.a, df.b, df.c).alias('r')).collect()
        assert res == [Row(r='')]

    def test_substr(self, spark):
        df = spark.createDataFrame(
            [
                (
                    "Spark SQL",
                    5,
                    1,
                )
            ],
            ["a", "b", "c"],
        )
        res = df.select(F.substr("a", "b", "c").alias("s")).collect()
        assert res == [Row(s='k')]

        df = spark.createDataFrame(
            [
                (
                    "Spark SQL",
                    5,
                    1,
                )
            ],
            ["a", "b", "c"],
        )
        res = df.select(F.substr("a", "b").alias("s")).collect()
        assert res == [Row(s='k SQL')]

    def test_find_in_set(self, spark):
        string_array = "abc,b,ab,c,def"
        df = spark.createDataFrame([("ab", string_array), ("b,c", string_array), ("z", string_array)], ['a', 'b'])

        res = df.select(F.find_in_set(df.a, df.b).alias('r')).collect()

        assert res == [Row(r=3), Row(r=0), Row(r=0)]

    def test_initcap(self, spark):
        df = spark.createDataFrame([('ab cd',)], ['a'])

        res = df.select(F.initcap("a").alias('v')).collect()
        assert res == [Row(v='Ab Cd')]

    def test_left(self, spark):
        df = spark.createDataFrame(
            [
                (
                    "Spark SQL",
                    3,
                ),
                (
                    "Spark SQL",
                    0,
                ),
                (
                    "Spark SQL",
                    -3,
                ),
            ],
            ['a', 'b'],
        )

        res = df.select(F.left(df.a, df.b).alias('r')).collect()
        assert res == [Row(r='Spa'), Row(r=''), Row(r='')]

    def test_right(self, spark):
        df = spark.createDataFrame(
            [
                (
                    "Spark SQL",
                    3,
                ),
                (
                    "Spark SQL",
                    0,
                ),
                (
                    "Spark SQL",
                    -3,
                ),
            ],
            ['a', 'b'],
        )

        res = df.select(F.right(df.a, df.b).alias('r')).collect()
        assert res == [Row(r='SQL'), Row(r=''), Row(r='')]

    def test_levenshtein(self, spark):
        df = spark.createDataFrame([("kitten", "sitting"), ("saturdays", "sunday")], ['a', 'b'])

        res = df.select(F.levenshtein(df.a, df.b).alias('r'), F.levenshtein(df.a, df.b, 3).alias('r_th')).collect()
        assert res == [Row(r=3, r_th=3), Row(r=4, r_th=-1)]

    def test_lpad(self, spark):
        df = spark.createDataFrame(
            [('abcd',)],
            [
                's',
            ],
        )

        res = df.select(F.lpad(df.s, 6, '#').alias('s')).collect()
        assert res == [Row(s='##abcd')]

    def test_rpad(self, spark):
        df = spark.createDataFrame(
            [('abcd',)],
            [
                's',
            ],
        )

        res = df.select(F.rpad(df.s, 6, '#').alias('s')).collect()
        assert res == [Row(s='abcd##')]

    def test_printf(self, spark):
        df = spark.createDataFrame(
            [
                (
                    "aa%d%s",
                    123,
                    "cc",
                )
            ],
            ["a", "b", "c"],
        )
        res = df.select(F.printf("a", "b", "c").alias("r")).collect()
        assert res == [Row(r='aa123cc')]

    @pytest.mark.parametrize("regexp_func", [F.regexp, F.regexp_like])
    def test_regexp_and_regexp_like(self, spark, regexp_func):
        df = spark.createDataFrame([("1a 2b 14m", r"(\d+)")], ["str", "regexp"])
        res = df.select(regexp_func('str', F.lit(r'(\d+)')).alias("m")).collect()
        assert res[0].m is True

        df = spark.createDataFrame([("1a 2b 14m", r"(\d+)")], ["str", "regexp"])
        res = df.select(regexp_func('str', F.lit(r'\d{2}b')).alias("m")).collect()
        assert res[0].m is False

        df = spark.createDataFrame([("1a 2b 14m", r"(\d+)")], ["str", "regexp"])
        res = df.select(regexp_func('str', F.col("regexp")).alias("m")).collect()
        assert res[0].m is True

    def test_regexp_count(self, spark):
        df = spark.createDataFrame([("1a 2b 14m", r"\d+")], ["str", "regexp"])
        res = df.select(F.regexp_count('str', F.lit(r'\d+')).alias('d')).collect()
        assert res == [Row(d=3)]
        res = df.select(F.regexp_count('str', F.lit(r'mmm')).alias('d')).collect()
        assert res == [Row(d=0)]
        res = df.select(F.regexp_count("str", F.col("regexp")).alias('d')).collect()
        assert res == [Row(d=3)]

    def test_regexp_extract(self, spark):
        df = spark.createDataFrame([('100-200',)], ['str'])
        res = df.select(F.regexp_extract('str', r'(\d+)-(\d+)', 1).alias('d')).collect()
        assert res == [Row(d='100')]

        df = spark.createDataFrame([('foo',)], ['str'])
        res = df.select(F.regexp_extract('str', r'(\d+)', 1).alias('d')).collect()
        assert res == [Row(d='')]

        df = spark.createDataFrame([('aaaac',)], ['str'])
        res = df.select(F.regexp_extract('str', '(a+)(b)?(c)', 2).alias('d')).collect()
        assert res == [Row(d='')]

    def test_regexp_extract_all(self, spark):
        df = spark.createDataFrame([("100-200, 300-400", r"(\d+)-(\d+)")], ["str", "regexp"])
        res = df.select(F.regexp_extract_all('str', F.lit(r'(\d+)-(\d+)')).alias('d')).collect()
        assert res == [Row(d=['100', '300'])]

        res = df.select(F.regexp_extract_all('str', F.lit(r'(\d+)-(\d+)'), 1).alias('d')).collect()
        assert res == [Row(d=['100', '300'])]

        res = df.select(F.regexp_extract_all('str', F.lit(r'(\d+)-(\d+)'), 2).alias('d')).collect()
        assert res == [Row(d=['200', '400'])]

        res = df.select(F.regexp_extract_all('str', F.col("regexp")).alias('d')).collect()
        assert res == [Row(d=['100', '300'])]

    def test_regexp_substr(self, spark):
        df = spark.createDataFrame([("1a 2b 14m", r"\d+")], ["str", "regexp"])

        res = df.select(F.regexp_substr('str', F.lit(r'\d+')).alias('d')).collect()
        assert res == [Row(d='1')]

        res = df.select(F.regexp_substr('str', F.lit(r'mmm')).alias('d')).collect()
        assert res == [Row(d=None)]

        res = df.select(F.regexp_substr("str", F.col("regexp")).alias('d')).collect()
        assert res == [Row(d='1')]

    def test_repeat(self, spark):
        df = spark.createDataFrame(
            [('ab',)],
            [
                's',
            ],
        )
        res = df.select(F.repeat(df.s, 3).alias('s')).collect()
        assert res == [Row(s='ababab')]

    def test_reverse(self, spark):
        data = [
            ("firstRowFirstColumn",),
            ("2ndRowFirstColumn",),
        ]
        df = spark.createDataFrame(data, ["firstColumn"])
        df = df.withColumn("reversed", F.reverse(F.col("firstColumn")))
        res = df.select("reversed").collect()
        assert res == [
            Row(reversed=data[0][0][::-1]),
            Row(reversed=data[1][0][::-1]),
        ]

    def test_concat(self, spark):
        data = [
            ("firstRow", "FirstColumn"),
            ("2ndRow", "FirstColumn"),
        ]
        df = spark.createDataFrame(data, ["firstColumn", "secondColumn"])
        df = df.withColumn("concatenated", F.concat(F.col("firstColumn"), F.col("secondColumn")))
        res = df.select("concatenated").collect()
        assert res == [
            Row(concatenated="firstRowFirstColumn"),
            Row(concatenated="2ndRowFirstColumn"),
        ]

    def test_substring(self, spark):
        data = [("abcd",)]
        df = spark.createDataFrame(data, ["s"])
        res = df.select(F.substring(df.s, 1, 2).alias("s")).collect()
        assert res == [Row(s="ab")]

    def test_contains(self, spark):
        data = [
            ("firstRowFirstColumn",),
            ("2ndRowFirstColumn",),
        ]
        df = spark.createDataFrame(data, ["firstColumn"])
        df = df.withColumn("contains", F.contains(F.col("firstColumn"), F.lit("Row")))
        res = df.select("contains").collect()
        assert res == [
            Row(contains=True),
            Row(contains=True),
        ]

    def test_instr(self, spark):
        data = [
            ("firstRowFirstColumn",),
            ("2ndRowFirstColumn",),
        ]
        df = spark.createDataFrame(data, ["firstColumn"])
        df = df.withColumn("instr", F.instr(F.col("firstColumn"), "Row"))
        res = df.select("instr").collect()
        assert res == [
            Row(instr=6),
            Row(instr=4),
        ]
