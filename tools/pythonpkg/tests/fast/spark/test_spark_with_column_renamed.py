import pytest

_ = pytest.importorskip("duckdb.spark")

from duckdb.spark.sql.types import (
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
from duckdb.spark.sql.functions import col, struct, when, lit
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
        from duckdb.spark.sql.types import StructType, StructField, StringType, IntegerType

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
        assert 'dob' not in df2
        assert 'salary' not in df2
        assert 'DateOfBirth' in df2
        assert 'salary_amount' in df2

        schema2 = StructType(
            [
                StructField("fname", StringType()),
                StructField("middlename", StringType()),
                StructField("lname", StringType()),
            ]
        )

        df2 = df.select(col("name").cast(schema2).alias("name"), col("dob"), col("gender"), col("salary"))
        assert 'firstname' not in df2.schema['name'].dataType
        assert 'fname' in df2.schema['name'].dataType

        df2 = df.select(
            col("name.firstname").alias("fname"),
            col("name.middlename").alias("mname"),
            col("name.lastname").alias("lname"),
            col("dob"),
            col("gender"),
            col("salary"),
        )
        assert 'firstname' not in df2
        assert 'fname' in df2
