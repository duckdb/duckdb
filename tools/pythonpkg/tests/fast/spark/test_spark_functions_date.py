import pytest

_ = pytest.importorskip("duckdb.experimental.spark")
from datetime import date, datetime

from duckdb.experimental.spark.sql import functions as F
from duckdb.experimental.spark.sql.types import Row


class TestsSparkFunctionsDate(object):
    def test_date_trunc(self, spark):
        df = spark.createDataFrame(
            [(datetime(2019, 1, 23, 14, 34, 9, 87539),)],
            ["dt_ref"],
        )

        expected = [
            {
                "year": datetime(2019, 1, 1),
                "yyyy": datetime(2019, 1, 1),
                "yy": datetime(2019, 1, 1),
                "quarter": datetime(2019, 1, 1),
                "month": datetime(2019, 1, 1),
                "mon": datetime(2019, 1, 1),
                "mm": datetime(2019, 1, 1),
                "week": datetime(2019, 1, 21),
                "day": datetime(2019, 1, 23),
                "dd": datetime(2019, 1, 23),
                "hour": datetime(2019, 1, 23, 14, 0, 0),
                "minute": datetime(2019, 1, 23, 14, 34, 0),
                "second": datetime(2019, 1, 23, 14, 34, 9),
            }
        ]

        cols = list(expected[0].keys())
        gen_record = df.select(
            *[F.date_trunc(fmt, "dt_ref").alias(fmt) for fmt in cols]
        ).collect()[0]

        expected_record = spark.createDataFrame(
            [r.values() for r in expected],
            cols,
        ).collect()[0]

        assert gen_record.year == expected_record.year
        assert gen_record.yyyy == expected_record.yyyy
        assert gen_record.yy == expected_record.yy
        assert gen_record.quarter == expected_record.quarter
        assert gen_record.month == expected_record.month
        assert gen_record.mon == expected_record.mon
        assert gen_record.mm == expected_record.mm
        assert gen_record.week == expected_record.week
        assert gen_record.day == expected_record.day
        assert gen_record.dd == expected_record.dd
        assert gen_record.hour == expected_record.hour
        assert gen_record.minute == expected_record.minute
        assert gen_record.second == expected_record.second

    def test_date_part(self, spark):
        df = spark.createDataFrame([(datetime(2015, 4, 8, 13, 8, 15),)], ["ts"])
        result = df.select(
            F.date_part(F.lit("YEAR"), "ts").alias("year"),
            F.date_part(F.lit("month"), "ts").alias("month"),
            F.date_part(F.lit("WEEK"), "ts").alias("week"),
            F.date_part(F.lit("D"), "ts").alias("day"),
            F.date_part(F.lit("M"), "ts").alias("minute"),
            F.date_part(F.lit("S"), "ts").alias("second"),
        ).collect()

        expected = [Row(year=2015, month=4, week=15, day=8, minute=8, second=15)]

        assert result == expected

    def test_dayofweek(self, spark):
        spark_sunday_index = 1
        spark_saturday_index = 7

        df = spark.createDataFrame([(date(2024, 5, 12),), (date(2024, 5, 18),)], ["dt"])
        result = df.select(F.dayofweek("dt").alias("week_num")).collect()

        assert result[0].week_num == spark_sunday_index
        assert result[1].week_num == spark_saturday_index

    def test_dayofmonth(self, spark):
        df = spark.createDataFrame([(date(2024, 5, 12),), (date(2024, 5, 18),)], ["dt"])
        result = df.select(F.dayofmonth("dt").alias("day_num")).collect()

        assert result[0].day_num == 12
        assert result[1].day_num == 18

    def test_dayofyear(self, spark):
        df = spark.createDataFrame([(date(2024, 5, 12),), (date(2024, 5, 18),)], ["dt"])
        result = df.select(F.dayofyear("dt").alias("day_num")).collect()

        assert result[0].day_num == 133
        assert result[1].day_num == 139

    def test_month(self, spark):
        df = spark.createDataFrame([(date(2024, 5, 12),), (date(2024, 5, 18),)], ["dt"])
        result = df.select(F.month("dt").alias("month_num")).collect()

        assert result[0].month_num == 5
        assert result[1].month_num == 5

    def test_year(self, spark):
        df = spark.createDataFrame([(date(2024, 5, 12),), (date(2024, 5, 18),)], ["dt"])
        result = df.select(F.year("dt").alias("year_num")).collect()

        assert result[0].year_num == 2024
        assert result[1].year_num == 2024

    def test_weekofyear(self, spark):
        df = spark.createDataFrame([(date(2024, 5, 12),), (date(2024, 5, 18),)], ["dt"])
        result = df.select(F.weekofyear("dt").alias("week_num")).collect()

        assert result[0].week_num == 19
        assert result[1].week_num == 20

    def test_quarter(self, spark):
        df = spark.createDataFrame([(date(2024, 5, 12),), (date(2024, 5, 18),)], ["dt"])
        result = df.select(F.quarter("dt").alias("quarter_num")).collect()

        assert result[0].quarter_num == 2
        assert result[1].quarter_num == 2

    def test_hour(self, spark):
        df = spark.createDataFrame([(datetime(2024, 5, 12, 13, 30, 45),)], ["dt"])
        result = df.select(F.hour("dt").alias("hour_num")).collect()

        assert result[0].hour_num == 13

    def test_minute(self, spark):
        df = spark.createDataFrame([(datetime(2024, 5, 12, 13, 30, 45),)], ["dt"])
        result = df.select(F.minute("dt").alias("minute_num")).collect()

        assert result[0].minute_num == 30

    def test_second(self, spark):
        df = spark.createDataFrame([(datetime(2024, 5, 12, 13, 30, 45),)], ["dt"])
        result = df.select(F.second("dt").alias("second_num")).collect()

        assert result[0].second_num == 45
