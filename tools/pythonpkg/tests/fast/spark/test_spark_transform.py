import pytest

_ = pytest.importorskip("duckdb.experimental.spark")

from spark_namespace.sql.types import (
    LongType,
    StructType,
    BooleanType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    Row,
    ArrayType,
    MapType,
)
from spark_namespace.sql.functions import col, struct, when, lit, array_contains
from spark_namespace.sql.functions import sum, avg, max, min, mean, count


@pytest.fixture
def array_df(spark):
    data = [
        ("James,,Smith", ["Java", "Scala", "C++"], ["Spark", "Java"]),
        ("Michael,Rose,", ["Spark", "Java", "C++"], ["Spark", "Java"]),
        ("Robert,,Williams", ["CSharp", "VB"], ["Spark", "Python"]),
    ]
    dataframe = spark.createDataFrame(data=data, schema=["Name", "Languages1", "Languages2"])
    yield dataframe


@pytest.fixture
def df(spark):
    simpleData = (
        ("Java", 4000, 5),
        ("Python", 4600, 10),
        ("Scala", 4100, 15),
        ("Scala", 4500, 15),
        ("PHP", 3000, 20),
    )
    columns = ["CourseName", "fee", "discount"]
    dataframe = spark.createDataFrame(data=simpleData, schema=columns)
    yield dataframe


class TestDataFrameUnion(object):
    def test_transform(self, spark, df):
        # Custom transformation 1
        from spark_namespace.sql.functions import upper

        def to_upper_str_columns(df):
            return df.withColumn("CourseName", upper(df.CourseName))

        # Custom transformation 2
        def reduce_price(df, reduceBy):
            return df.withColumn("new_fee", df.fee - reduceBy)

        # Custom transformation 3
        def apply_discount(df):
            return df.withColumn("discounted_fee", df.new_fee - (df.new_fee * df.discount) / 100)

        df2 = df.transform(to_upper_str_columns).transform(reduce_price, 1000).transform(apply_discount)
        res = df2.collect()
        assert res == [
            Row(CourseName='JAVA', fee=4000, discount=5, new_fee=3000, discounted_fee=2850.0),
            Row(CourseName='PYTHON', fee=4600, discount=10, new_fee=3600, discounted_fee=3240.0),
            Row(CourseName='SCALA', fee=4100, discount=15, new_fee=3100, discounted_fee=2635.0),
            Row(CourseName='SCALA', fee=4500, discount=15, new_fee=3500, discounted_fee=2975.0),
            Row(CourseName='PHP', fee=3000, discount=20, new_fee=2000, discounted_fee=1600.0),
        ]

    # https://sparkbyexamples.com/pyspark/pyspark-transform-function/
    @pytest.mark.skip(reason='LambdaExpressions are currently under development, waiting til that is finished')
    def test_transform_function(self, spark, array_df):
        from spark_namespace.sql.functions import upper, transform

        df.select(transform("Languages1", lambda x: upper(x)).alias("languages1")).show()
