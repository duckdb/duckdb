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
from pyduckdb.spark.sql.functions import sum, avg, max, min, mean, count


class TestDataFrameJoin(object):
    def test_join(self, spark):
        emp = [
            (1, "Smith", -1, "2018", "10", "M", 3000),
            (2, "Rose", 1, "2010", "20", "M", 4000),
            (3, "Williams", 1, "2010", "10", "M", 1000),
            (4, "Jones", 2, "2005", "10", "F", 2000),
            (5, "Brown", 2, "2010", "40", "", -1),
            (6, "Brown", 2, "2010", "50", "", -1),
        ]
        empColumns = ["emp_id", "name", "superior_emp_id", "year_joined", "emp_dept_id", "gender", "salary"]

        empDF = spark.createDataFrame(data=emp, schema=empColumns)

        dept = [("Finance", 10), ("Marketing", 20), ("Sales", 30), ("IT", 40)]
        deptColumns = ["dept_name", "dept_id"]
        deptDF = spark.createDataFrame(data=dept, schema=deptColumns)

        df2 = empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "inner")
        res = df2.collect()
        assert res == [
            Row(
                emp_id=1,
                name='Smith',
                superior_emp_id=-1,
                year_joined='2018',
                emp_dept_id='10',
                gender='M',
                salary=3000,
                dept_name='Finance',
                dept_id=10,
            ),
            Row(
                emp_id=2,
                name='Rose',
                superior_emp_id=1,
                year_joined='2010',
                emp_dept_id='20',
                gender='M',
                salary=4000,
                dept_name='Marketing',
                dept_id=20,
            ),
            Row(
                emp_id=3,
                name='Williams',
                superior_emp_id=1,
                year_joined='2010',
                emp_dept_id='10',
                gender='M',
                salary=1000,
                dept_name='Finance',
                dept_id=10,
            ),
            Row(
                emp_id=4,
                name='Jones',
                superior_emp_id=2,
                year_joined='2005',
                emp_dept_id='10',
                gender='F',
                salary=2000,
                dept_name='Finance',
                dept_id=10,
            ),
            Row(
                emp_id=5,
                name='Brown',
                superior_emp_id=2,
                year_joined='2010',
                emp_dept_id='40',
                gender='',
                salary=-1,
                dept_name='IT',
                dept_id=40,
            ),
        ]
