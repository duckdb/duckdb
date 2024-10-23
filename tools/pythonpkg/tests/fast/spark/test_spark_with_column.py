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
from spark_namespace.sql.functions import col, struct, when, lit
from spark_namespace import USE_ACTUAL_SPARK
import duckdb
import re


class TestWithColumn(object):
    def test_with_column(self, spark):
        data = [
            ('James', '', 'Smith', '1991-04-01', 'M', 3000),
            ('Michael', 'Rose', '', '2000-05-19', 'M', 4000),
            ('Robert', '', 'Williams', '1978-09-05', 'M', 4000),
            ('Maria', 'Anne', 'Jones', '1967-12-01', 'F', 4000),
            ('Jen', 'Mary', 'Brown', '1980-02-17', 'F', -1),
        ]

        columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]
        df = spark.createDataFrame(data=data, schema=columns)
        assert df.schema['salary'].dataType.typeName() == ('long' if USE_ACTUAL_SPARK else 'integer')

        # The type of 'salary' has been cast to Bigint
        new_df = df.withColumn("salary", col("salary").cast("BIGINT"))
        assert new_df.schema['salary'].dataType.typeName() == 'long'

        # Replace the 'salary' column with '(salary * 100)'
        df2 = df.withColumn("salary", col("salary") * 100)
        res = df2.collect()
        assert res[0].salary == 300_000

        # Create a column from an existing column
        df2 = df.withColumn("CopiedColumn", col("salary") * -1)
        res = df2.collect()
        assert res[0].CopiedColumn == -3000

        df2 = df.withColumn("Country", lit("USA"))
        res = df2.collect()
        assert res[0].Country == 'USA'

        df2 = df.withColumn("Country", lit("USA")).withColumn("anotherColumn", lit("anotherValue"))
        res = df2.collect()
        assert res[0].Country == 'USA'
        assert res[1].anotherColumn == 'anotherValue'

        df2 = df.withColumnRenamed("gender", "sex")
        assert 'gender' not in df2.columns
        assert 'sex' in df2.columns

        df2 = df.drop("salary")
        assert 'salary' not in df2.columns
