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

    def test_octet_length(self, spark):
        df = spark.createDataFrame([('cat',)], ['c1'])
        res = df.select(F.octet_length('c1').alias("o")).collect()
        assert res == [Row(o=3)]

    def test_positive(self, spark):
        df = spark.createDataFrame([(-1,), (0,), (1,)], ['v'])
        res = df.select(F.positive("v").alias("p")).collect()
        assert res == [Row(p=-1), Row(p=0), Row(p=1)]

    def test_sequence(self, spark):
        df1 = spark.createDataFrame([(-2, 2)], ('C1', 'C2'))
        res = df1.select(F.sequence('C1', 'C2').alias('r')).collect()
        assert res == [Row(r=[-2, -1, 0, 1, 2])]

        df2 = spark.createDataFrame([(4, -4, -2)], ('C1', 'C2', 'C3'))
        res = df2.select(F.sequence('C1', 'C2', 'C3').alias('r')).collect()
        assert res == [Row(r=[4, 2, 0, -2, -4])]

    def test_like(self, spark):
        df = spark.createDataFrame([("Spark", "_park")], ['a', 'b'])
        res = df.select(F.like(df.a, df.b).alias('r')).collect()
        assert res == [Row(r=True)]

        df = spark.createDataFrame([("%SystemDrive%/Users/John", "/%SystemDrive/%//Users%")], ['a', 'b'])
        res = df.select(F.like(df.a, df.b, F.lit('/')).alias('r')).collect()
        assert res == [Row(r=True)]

    def test_ilike(self, spark):
        df = spark.createDataFrame([("Spark", "spark")], ['a', 'b'])
        res = df.select(F.ilike(df.a, df.b).alias('r')).collect()
        assert res == [Row(r=True)]

        df = spark.createDataFrame([("%SystemDrive%/Users/John", "/%SystemDrive/%//Users%")], ['a', 'b'])
        res = df.select(F.ilike(df.a, df.b, F.lit('/')).alias('r')).collect()
        assert res == [Row(r=True)]
