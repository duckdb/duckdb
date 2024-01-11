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
from duckdb.experimental.spark.sql.functions import sum, avg, max, min, mean, count


@pytest.fixture
def dataframe_a(spark):
    emp = [
        (1, "Smith", -1, "2018", "10", "M", 3000),
        (2, "Rose", 1, "2010", "20", "M", 4000),
        (3, "Williams", 1, "2010", "10", "M", 1000),
        (4, "Jones", 2, "2005", "10", "F", 2000),
        (5, "Brown", 2, "2010", "40", "", -1),
        (6, "Brown", 2, "2010", "50", "", -1),
    ]
    empColumns = ["emp_id", "name", "superior_emp_id", "year_joined", "emp_dept_id", "gender", "salary"]
    dataframe = spark.createDataFrame(data=emp, schema=empColumns)
    yield dataframe


@pytest.fixture
def dataframe_b(spark):
    dept = [("Finance", 10), ("Marketing", 20), ("Sales", 30), ("IT", 40)]
    deptColumns = ["dept_name", "dept_id"]
    dataframe = spark.createDataFrame(data=dept, schema=deptColumns)
    yield dataframe


class TestDataFrameJoin(object):
    def test_inner_join(self, dataframe_a, dataframe_b):
        df = dataframe_a.join(dataframe_b, dataframe_a.emp_dept_id == dataframe_b.dept_id, "inner")
        res = df.collect()
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

    @pytest.mark.parametrize('how', ['outer', 'fullouter', 'full', 'full_outer'])
    def test_outer_join(self, dataframe_a, dataframe_b, how):
        df = dataframe_a.join(dataframe_b, dataframe_a.emp_dept_id == dataframe_b.dept_id, how)
        res1 = df.collect()
        assert res1 == [
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
            Row(
                emp_id=6,
                name='Brown',
                superior_emp_id=2,
                year_joined='2010',
                emp_dept_id='50',
                gender='',
                salary=-1,
                dept_name=None,
                dept_id=None,
            ),
            Row(
                emp_id=None,
                name=None,
                superior_emp_id=None,
                year_joined=None,
                emp_dept_id=None,
                gender=None,
                salary=None,
                dept_name='Sales',
                dept_id=30,
            ),
        ]

    @pytest.mark.parametrize('how', ['right', 'rightouter', 'right_outer'])
    def test_right_join(self, dataframe_a, dataframe_b, how):
        df = dataframe_a.join(dataframe_b, dataframe_a.emp_dept_id == dataframe_b.dept_id, how)
        res = df.collect()
        assert res == [
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
                emp_id=None,
                name=None,
                superior_emp_id=None,
                year_joined=None,
                emp_dept_id=None,
                gender=None,
                salary=None,
                dept_name='Sales',
                dept_id=30,
            ),
        ]

    @pytest.mark.parametrize('how', ['semi', 'leftsemi', 'left_semi'])
    def test_semi_join(self, dataframe_a, dataframe_b, how):
        df = dataframe_a.join(dataframe_b, dataframe_a.emp_dept_id == dataframe_b.dept_id, how)
        res = df.collect()
        assert res == [
            Row(
                emp_id=1,
                name='Smith',
                superior_emp_id=-1,
                year_joined='2018',
                emp_dept_id='10',
                gender='M',
                salary=3000,
            ),
            Row(
                emp_id=2, name='Rose', superior_emp_id=1, year_joined='2010', emp_dept_id='20', gender='M', salary=4000
            ),
            Row(
                emp_id=3,
                name='Williams',
                superior_emp_id=1,
                year_joined='2010',
                emp_dept_id='10',
                gender='M',
                salary=1000,
            ),
            Row(
                emp_id=4, name='Jones', superior_emp_id=2, year_joined='2005', emp_dept_id='10', gender='F', salary=2000
            ),
            Row(emp_id=5, name='Brown', superior_emp_id=2, year_joined='2010', emp_dept_id='40', gender='', salary=-1),
        ]

    @pytest.mark.parametrize('how', ['anti', 'leftanti', 'left_anti'])
    def test_anti_join(self, dataframe_a, dataframe_b, how):
        df = dataframe_a.join(dataframe_b, dataframe_a.emp_dept_id == dataframe_b.dept_id, how)
        res = df.collect()
        assert res == [
            Row(emp_id=6, name='Brown', superior_emp_id=2, year_joined='2010', emp_dept_id='50', gender='', salary=-1)
        ]

    def test_self_join(self, dataframe_a):
        empDF = dataframe_a

        df = (
            empDF.alias("emp1")
            .join(empDF.alias("emp2"), col("emp1.superior_emp_id") == col("emp2.emp_id"), "inner")
            .select(
                col("emp1.emp_id"),
                col("emp1.name"),
                col("emp2.emp_id").alias("superior_emp_id"),
                col("emp2.name").alias("superior_emp_name"),
            )
        )
        res = df.collect()
        assert res == [
            Row(emp_id=2, name='Rose', superior_emp_id=1, superior_emp_name='Smith'),
            Row(emp_id=3, name='Williams', superior_emp_id=1, superior_emp_name='Smith'),
            Row(emp_id=4, name='Jones', superior_emp_id=2, superior_emp_name='Rose'),
            Row(emp_id=5, name='Brown', superior_emp_id=2, superior_emp_name='Rose'),
            Row(emp_id=6, name='Brown', superior_emp_id=2, superior_emp_name='Rose'),
        ]
