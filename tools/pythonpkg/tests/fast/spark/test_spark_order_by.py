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

    def test_null_ordering(self, spark):
        df = spark.createDataFrame([(3,), (None,), (2,)], ("value",))

        res = df.orderBy("value").collect()
        assert res == [Row(value=None), Row(value=2), Row(value=3)]

        res = df.orderBy("value", ascending=True).collect()
        assert res == [Row(value=None), Row(value=2), Row(value=3)]

        res = df.orderBy("value", ascending=False).collect()
        assert res == [Row(value=3), Row(value=2), Row(value=None)]

        res = df.orderBy(df.value).collect()
        assert res == [Row(value=None), Row(value=2), Row(value=3)]

        res = df.orderBy(df.value.asc()).collect()
        assert res == [Row(value=None), Row(value=2), Row(value=3)]

        res = df.orderBy(df.value.desc()).collect()
        assert res == [Row(value=3), Row(value=2), Row(value=None)]

        df = spark.createDataFrame([(3, "A"), (3, None), (None, "A"), (2, "A")], ("value1", "value2"))

        res = df.orderBy("value1", "value2").collect()
        assert res == [
            Row(value1=None, value2='A'),
            Row(value1=2, value2='A'),
            Row(value1=3, value2=None),
            Row(value1=3, value2='A'),
        ]

        res = df.orderBy("value1", "value2", ascending=True).collect()
        assert res == [
            Row(value1=None, value2='A'),
            Row(value1=2, value2='A'),
            Row(value1=3, value2=None),
            Row(value1=3, value2='A'),
        ]

        res = df.orderBy("value1", "value2", ascending=False).collect()
        assert res == [
            Row(value1=3, value2='A'),
            Row(value1=3, value2=None),
            Row(value1=2, value2='A'),
            Row(value1=None, value2='A'),
        ]

        res = df.orderBy(df.value1, df.value2).collect()
        assert res == [
            Row(value1=None, value2='A'),
            Row(value1=2, value2='A'),
            Row(value1=3, value2=None),
            Row(value1=3, value2='A'),
        ]

        res = df.orderBy(df.value1.asc(), df.value2.asc()).collect()
        assert res == [
            Row(value1=None, value2='A'),
            Row(value1=2, value2='A'),
            Row(value1=3, value2=None),
            Row(value1=3, value2='A'),
        ]

        res = df.orderBy(df.value1.desc(), df.value2.desc()).collect()
        assert res == [
            Row(value1=3, value2='A'),
            Row(value1=3, value2=None),
            Row(value1=2, value2='A'),
            Row(value1=None, value2='A'),
        ]

        res = df.orderBy(df.value1, df.value2, ascending=[True, False]).collect()
        assert res == [
            Row(value1=None, value2='A'),
            Row(value1=2, value2='A'),
            Row(value1=3, value2="A"),
            Row(value1=3, value2=None),
        ]
