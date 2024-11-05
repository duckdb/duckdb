import pytest

_ = pytest.importorskip("duckdb.experimental.spark")

import math
import numpy as np
from spark_namespace import USE_ACTUAL_SPARK
from spark_namespace.sql import functions as F
from spark_namespace.sql.types import Row


class TestSparkFunctionsNumeric(object):
    def test_greatest(self, spark):
        data = [
            (1, 2),
            (4, 3),
        ]
        df = spark.createDataFrame(data, ["firstColumn", "secondColumn"])
        df = df.withColumn("greatest_value", F.greatest(F.col("firstColumn"), F.col("secondColumn")))
        res = df.select("greatest_value").collect()
        assert res == [
            Row(greatest_value=2),
            Row(greatest_value=4),
        ]

    def test_least(self, spark):
        data = [
            (1, 2),
            (4, 3),
        ]
        df = spark.createDataFrame(data, ["firstColumn", "secondColumn"])
        df = df.withColumn("least_value", F.least(F.col("firstColumn"), F.col("secondColumn")))
        res = df.select("least_value").collect()
        assert res == [
            Row(least_value=1),
            Row(least_value=3),
        ]

    def test_ceil(self, spark):
        data = [
            (1.1,),
            (2.9,),
        ]
        df = spark.createDataFrame(data, ["firstColumn"])
        df = df.withColumn("ceil_value", F.ceil(F.col("firstColumn")))
        res = df.select("ceil_value").collect()
        assert res == [
            Row(ceil_value=2),
            Row(ceil_value=3),
        ]

    def test_floor(self, spark):
        data = [
            (1.1,),
            (2.9,),
        ]
        df = spark.createDataFrame(data, ["firstColumn"])
        df = df.withColumn("floor_value", F.floor(F.col("firstColumn")))
        res = df.select("floor_value").collect()
        assert res == [
            Row(floor_value=1),
            Row(floor_value=2),
        ]

    def test_abs(self, spark):
        data = [
            (1.1,),
            (-2.9,),
        ]
        df = spark.createDataFrame(data, ["firstColumn"])
        df = df.withColumn("abs_value", F.abs(F.col("firstColumn")))
        res = df.select("abs_value").collect()
        assert res == [
            Row(abs_value=1.1),
            Row(abs_value=2.9),
        ]

    def test_sqrt(self, spark):
        data = [
            (4,),
            (9,),
        ]
        df = spark.createDataFrame(data, ["firstColumn"])
        df = df.withColumn("sqrt_value", F.sqrt(F.col("firstColumn")))
        res = df.select("sqrt_value").collect()
        assert res == [
            Row(sqrt_value=2.0),
            Row(sqrt_value=3.0),
        ]

    def test_cbrt(self, spark):
        data = [
            (8,),
            (27,),
        ]
        df = spark.createDataFrame(data, ["firstColumn"])
        df = df.withColumn("cbrt_value", F.cbrt(F.col("firstColumn")))
        res = df.select("cbrt_value").collect()
        assert pytest.approx(res[0].cbrt_value) == 2.0
        assert pytest.approx(res[1].cbrt_value) == 3.0

    def test_cos(self, spark):
        data = [
            (0.0,),
            (3.14159,),
        ]
        df = spark.createDataFrame(data, ["firstColumn"])
        df = df.withColumn("cos_value", F.cos(F.col("firstColumn")))
        res = df.select("cos_value").collect()
        assert len(res) == 2
        assert res[0].cos_value == pytest.approx(1.0)
        assert res[1].cos_value == pytest.approx(-1.0)

    def test_acos(self, spark):
        data = [
            (1,),
            (-1,),
        ]
        df = spark.createDataFrame(data, ["firstColumn"])
        df = df.withColumn("acos_value", F.acos(F.col("firstColumn")))
        res = df.select("acos_value").collect()
        assert len(res) == 2
        assert res[0].acos_value == pytest.approx(0.0)
        assert res[1].acos_value == pytest.approx(3.141592653589793)

    def test_exp(self, spark):
        data = [
            (0.693,),
            (0.0,),
        ]
        df = spark.createDataFrame(data, ["firstColumn"])
        df = df.withColumn("exp_value", F.exp(F.col("firstColumn")))
        res = df.select("exp_value").collect()
        round(res[0].exp_value, 2) == 2
        res[1].exp_value == 1

    def test_factorial(self, spark):
        data = [
            (4,),
            (5,),
        ]
        df = spark.createDataFrame(data, ["firstColumn"])
        df = df.withColumn("factorial_value", F.factorial(F.col("firstColumn")))
        res = df.select("factorial_value").collect()
        assert res == [
            Row(factorial_value=24),
            Row(factorial_value=120),
        ]

    def test_log2(self, spark):
        data = [
            (4,),
            (8,),
        ]
        df = spark.createDataFrame(data, ["firstColumn"])
        df = df.withColumn("log2_value", F.log2(F.col("firstColumn")))
        res = df.select("log2_value").collect()
        assert res == [
            Row(log2_value=2.0),
            Row(log2_value=3.0),
        ]

    def test_ln(self, spark):
        data = [
            (2.718,),
            (1.0,),
        ]
        df = spark.createDataFrame(data, ["firstColumn"])
        df = df.withColumn("ln_value", F.ln(F.col("firstColumn")))
        res = df.select("ln_value").collect()
        round(res[0].ln_value, 2) == 1
        res[1].ln_value == 0

    def test_degrees(self, spark):
        data = [
            (3.14159,),
            (0.0,),
        ]
        df = spark.createDataFrame(data, ["firstColumn"])
        df = df.withColumn("degrees_value", F.degrees(F.col("firstColumn")))
        res = df.select("degrees_value").collect()
        round(res[0].degrees_value, 2) == 180
        res[1].degrees_value == 0

    def test_radians(self, spark):
        data = [
            (180,),
            (0,),
        ]
        df = spark.createDataFrame(data, ["firstColumn"])
        df = df.withColumn("radians_value", F.radians(F.col("firstColumn")))
        res = df.select("radians_value").collect()
        round(res[0].radians_value, 2) == 3.14
        res[1].radians_value == 0

    def test_atan(self, spark):
        data = [
            (1,),
            (0,),
        ]
        df = spark.createDataFrame(data, ["firstColumn"])
        df = df.withColumn("atan_value", F.atan(F.col("firstColumn")))
        res = df.select("atan_value").collect()
        round(res[0].atan_value, 2) == 0.79
        res[1].atan_value == 0

    def test_atan2(self, spark):
        data = [
            (1, 1),
            (0, 0),
        ]
        df = spark.createDataFrame(data, ["firstColumn", "secondColumn"])

        # Both columns
        df2 = df.withColumn("atan2_value", F.atan2(F.col("firstColumn"), "secondColumn"))
        res = df2.select("atan2_value").collect()
        round(res[0].atan2_value, 2) == 0.79
        res[1].atan2_value == 0

        # Both literals
        df2 = df.withColumn("atan2_value_lit", F.atan2(1, 1))
        res = df2.select("atan2_value_lit").collect()
        round(res[0].atan2_value_lit, 2) == 0.79
        round(res[1].atan2_value_lit, 2) == 0.79

        # One literal, one column
        df2 = df.withColumn("atan2_value_lit_col", F.atan2(1.0, F.col("secondColumn")))
        res = df2.select("atan2_value_lit_col").collect()
        round(res[0].atan2_value_lit_col, 2) == 0.79
        res[1].atan2_value_lit_col == 0

    def test_tan(self, spark):
        data = [
            (0,),
            (1,),
        ]
        df = spark.createDataFrame(data, ["firstColumn"])
        df = df.withColumn("tan_value", F.tan(F.col("firstColumn")))
        res = df.select("tan_value").collect()
        res[0].tan_value == 0
        round(res[1].tan_value, 2) == 1.56

    def test_round(self, spark):
        data = [
            (11.15,),
            (2.9,),
            # Test with this that HALF_UP rounding method is used and not HALF_EVEN
            (2.5,),
        ]
        df = spark.createDataFrame(data, ["firstColumn"])
        df = (
            df.withColumn("round_value", F.round("firstColumn"))
            .withColumn("round_value_1", F.round(F.col("firstColumn"), 1))
            .withColumn("round_value_minus_1", F.round("firstColumn", -1))
        )
        res = df.select("round_value", "round_value_1", "round_value_minus_1").collect()
        assert res == [
            Row(round_value=11, round_value_1=11.2, round_value_minus_1=10),
            Row(round_value=3, round_value_1=2.9, round_value_minus_1=0),
            Row(round_value=3, round_value_1=2.5, round_value_minus_1=0),
        ]

    def test_bround(self, spark):
        data = [
            (11.15,),
            (2.9,),
            # Test with this that HALF_EVEN rounding method is used and not HALF_UP
            (2.5,),
        ]
        df = spark.createDataFrame(data, ["firstColumn"])
        df = (
            df.withColumn("round_value", F.bround(F.col("firstColumn")))
            .withColumn("round_value_1", F.bround(F.col("firstColumn"), 1))
            .withColumn("round_value_minus_1", F.bround(F.col("firstColumn"), -1))
        )
        res = df.select("round_value", "round_value_1", "round_value_minus_1").collect()
        assert res == [
            Row(round_value=11, round_value_1=11.2, round_value_minus_1=10),
            Row(round_value=3, round_value_1=2.9, round_value_minus_1=0),
            Row(round_value=2, round_value_1=2.5, round_value_minus_1=0),
        ]

    def test_asin(self, spark):
        df = spark.createDataFrame([(0,), (2,)], ["value"])

        df = df.withColumn("asin_value", F.asin("value"))
        res = df.select("asin_value").collect()

        assert res[0].asin_value == 0
        if USE_ACTUAL_SPARK:
            assert np.isnan(res[1].asin_value)
        else:
            # FIXME: DuckDB should return NaN here. Reason is that
            # ConstantExpression(float("nan")) gives NULL and not NaN
            assert res[1].asin_value is None

    def test_corr(self, spark):
        N = 20
        a = range(N)
        b = [2 * x for x in range(N)]
        # Have to use a groupby to test this as agg is not yet implemented without
        df = spark.createDataFrame(zip(a, b, ["group1"] * N), ["a", "b", "g"])

        res = df.groupBy("g").agg(F.corr("a", "b").alias('c')).collect()
        assert pytest.approx(res[0].c) == 1

    def test_cot(self, spark):
        df = spark.createDataFrame([(math.radians(45),)], ["value"])

        res = df.select(F.cot(df["value"]).alias("cot")).collect()
        assert pytest.approx(res[0].cot) == 1

    def test_e(self, spark):
        df = spark.createDataFrame([("value",)], ["value"])

        res = df.select(F.e().alias("e")).collect()
        assert pytest.approx(res[0].e) == math.e
