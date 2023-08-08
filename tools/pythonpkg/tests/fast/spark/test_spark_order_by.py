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


class TestDataFrameOrderBy(object):
    def test_order_by(self, spark):
        simpleData = [
            ("James", "Sales", "NY", 90000, 34, 10000),
            ("Michael", "Sales", "NY", 86000, 56, 20000),
            ("Robert", "Sales", "CA", 81000, 30, 23000),
            ("Maria", "Finance", "CA", 90000, 24, 23000),
            ("Raman", "Finance", "CA", 99000, 40, 24000),
            ("Scott", "Finance", "NY", 83000, 36, 19000),
            ("Jen", "Finance", "NY", 79000, 53, 15000),
            ("Jeff", "Marketing", "CA", 80000, 25, 18000),
            ("Kumar", "Marketing", "NY", 91000, 50, 21000),
        ]
        columns = ["employee_name", "department", "state", "salary", "age", "bonus"]
        df = spark.createDataFrame(data=simpleData, schema=columns)

        df2 = df.sort("department", "state")
        res = df2.collect()
        print(res)

        df2 = df.sort(col("department"), col("state"))
        res = df2.collect()
        print(res)
