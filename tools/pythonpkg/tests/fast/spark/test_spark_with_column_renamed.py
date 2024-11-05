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
import duckdb
import re


class TestWithColumnRenamed(object):
    def test_with_column_renamed(self, spark):
        dataDF = [
            (('James', '', 'Smith'), '1991-04-01', 'M', 3000),
            (('Michael', 'Rose', ''), '2000-05-19', 'M', 4000),
            (('Robert', '', 'Williams'), '1978-09-05', 'M', 4000),
            (('Maria', 'Anne', 'Jones'), '1967-12-01', 'F', 4000),
            (('Jen', 'Mary', 'Brown'), '1980-02-17', 'F', -1),
        ]
        from spark_namespace.sql.types import StructType, StructField, StringType, IntegerType

        schema = StructType(
            [
                StructField(
                    'name',
                    StructType(
                        [
                            StructField('firstname', StringType(), True),
                            StructField('middlename', StringType(), True),
                            StructField('lastname', StringType(), True),
                        ]
                    ),
                ),
                StructField('dob', StringType(), True),
                StructField('gender', StringType(), True),
                StructField('salary', IntegerType(), True),
            ]
        )

        df = spark.createDataFrame(data=dataDF, schema=schema)

        df2 = df.withColumnRenamed("dob", "DateOfBirth").withColumnRenamed("salary", "salary_amount")
        assert 'dob' not in df2.columns
        assert 'salary' not in df2.columns
        assert 'DateOfBirth' in df2.columns
        assert 'salary_amount' in df2.columns

        schema2 = StructType(
            [
                StructField(
                    'full name',
                    StructType(
                        [
                            StructField('fname', StringType(), True),
                            StructField('mname', StringType(), True),
                            StructField('lname', StringType(), True),
                        ]
                    ),
                ),
            ]
        )

        df2 = df.withColumnRenamed("name", "full name")
        assert 'name' not in df2.columns
        assert 'full name' in df2.columns
        assert 'firstname' in df2.schema['full name'].dataType.fieldNames()

        df2 = df.select(
            col("name").alias("full name"),
            col("dob"),
            col("gender"),
            col("salary"),
        )
        assert 'name' not in df2.columns
        assert 'full name' in df2.columns
        assert 'firstname' in df2.schema['full name'].dataType.fieldNames()

        df2 = df.select(
            col("name.firstname").alias("fname"),
            col("name.middlename").alias("mname"),
            col("name.lastname").alias("lname"),
            col("dob"),
            col("gender"),
            col("salary"),
        )
        assert 'firstname' not in df2.columns
        assert 'fname' in df2.columns
