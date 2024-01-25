import pytest

_ = pytest.importorskip("duckdb.experimental.spark")

from duckdb.experimental.spark.sql.types import (
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
from duckdb.experimental.spark.sql.functions import col, struct, when, lit, array_contains
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
        res1 = df2.collect()
        assert res1 == [
            Row(employee_name='Maria', department='Finance', state='CA', salary=90000, age=24, bonus=23000),
            Row(employee_name='Raman', department='Finance', state='CA', salary=99000, age=40, bonus=24000),
            Row(employee_name='Scott', department='Finance', state='NY', salary=83000, age=36, bonus=19000),
            Row(employee_name='Jen', department='Finance', state='NY', salary=79000, age=53, bonus=15000),
            Row(employee_name='Jeff', department='Marketing', state='CA', salary=80000, age=25, bonus=18000),
            Row(employee_name='Kumar', department='Marketing', state='NY', salary=91000, age=50, bonus=21000),
            Row(employee_name='Robert', department='Sales', state='CA', salary=81000, age=30, bonus=23000),
            Row(employee_name='James', department='Sales', state='NY', salary=90000, age=34, bonus=10000),
            Row(employee_name='Michael', department='Sales', state='NY', salary=86000, age=56, bonus=20000),
        ]

        df2 = df.sort(col("department"), col("state"))
        res2 = df2.collect()
        assert res2 == res1

        df2 = df.orderBy(col("department").asc(), col("state").asc())
        res3 = df2.collect()
        assert res3 == res1

        df2 = df.sort(df.department.asc(), df.state.desc())
        res1 = df2.collect()
        assert res1 == [
            Row(employee_name='Scott', department='Finance', state='NY', salary=83000, age=36, bonus=19000),
            Row(employee_name='Jen', department='Finance', state='NY', salary=79000, age=53, bonus=15000),
            Row(employee_name='Maria', department='Finance', state='CA', salary=90000, age=24, bonus=23000),
            Row(employee_name='Raman', department='Finance', state='CA', salary=99000, age=40, bonus=24000),
            Row(employee_name='Kumar', department='Marketing', state='NY', salary=91000, age=50, bonus=21000),
            Row(employee_name='Jeff', department='Marketing', state='CA', salary=80000, age=25, bonus=18000),
            Row(employee_name='James', department='Sales', state='NY', salary=90000, age=34, bonus=10000),
            Row(employee_name='Michael', department='Sales', state='NY', salary=86000, age=56, bonus=20000),
            Row(employee_name='Robert', department='Sales', state='CA', salary=81000, age=30, bonus=23000),
        ]

        df2 = df.sort(col("department").asc(), col("state").desc())
        res2 = df2.collect()
        assert res2 == res1

        df2 = df.orderBy(col("department").asc(), col("state").desc())
        res3 = df2.collect()
        assert res3 == res1

        df.createOrReplaceTempView("EMP")
        df2 = spark.sql(
            """
            select
                employee_name,
                department,
                state,
                salary,
                age,
                bonus
            from EMP ORDER BY department asc
        """
        )
        res = df2.collect()
        assert res == [
            Row(employee_name='Maria', department='Finance', state='CA', salary=90000, age=24, bonus=23000),
            Row(employee_name='Raman', department='Finance', state='CA', salary=99000, age=40, bonus=24000),
            Row(employee_name='Scott', department='Finance', state='NY', salary=83000, age=36, bonus=19000),
            Row(employee_name='Jen', department='Finance', state='NY', salary=79000, age=53, bonus=15000),
            Row(employee_name='Jeff', department='Marketing', state='CA', salary=80000, age=25, bonus=18000),
            Row(employee_name='Kumar', department='Marketing', state='NY', salary=91000, age=50, bonus=21000),
            Row(employee_name='James', department='Sales', state='NY', salary=90000, age=34, bonus=10000),
            Row(employee_name='Michael', department='Sales', state='NY', salary=86000, age=56, bonus=20000),
            Row(employee_name='Robert', department='Sales', state='CA', salary=81000, age=30, bonus=23000),
        ]
