import pytest

_ = pytest.importorskip("pyduckdb.spark")

from pyduckdb.spark.sql.types import (
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
from pyduckdb.spark.sql.functions import col, struct, when, lit, array_contains
import duckdb
import re


class TestDataFrameDropDuplicates(object):
    def test_spark_drop_duplicates(self, spark):
        # Prepare Data
        data = [
            ("James", "Sales", 3000),
            ("Michael", "Sales", 4600),
            ("Robert", "Sales", 4100),
            ("Maria", "Finance", 3000),
            ("James", "Sales", 3000),
            ("Scott", "Finance", 3300),
            ("Jen", "Finance", 3900),
            ("Jeff", "Marketing", 3000),
            ("Kumar", "Marketing", 2000),
            ("Saif", "Sales", 4100),
        ]

        # Create DataFrame
        columns = ["employee_name", "department", "salary"]
        df = spark.createDataFrame(data=data, schema=columns)

        distinctDF = df.distinct()
        print("Distinct count: " + str(distinctDF.count()))
        res = distinctDF.collect()
        # James | Sales had a duplicate, has been removed
        assert res == [
            Row(employee_name='James', department='Sales', salary=3000),
            Row(employee_name='Michael', department='Sales', salary=4600),
            Row(employee_name='Robert', department='Sales', salary=4100),
            Row(employee_name='Maria', department='Finance', salary=3000),
            Row(employee_name='Scott', department='Finance', salary=3300),
            Row(employee_name='Jen', department='Finance', salary=3900),
            Row(employee_name='Jeff', department='Marketing', salary=3000),
            Row(employee_name='Kumar', department='Marketing', salary=2000),
            Row(employee_name='Saif', department='Sales', salary=4100),
        ]
