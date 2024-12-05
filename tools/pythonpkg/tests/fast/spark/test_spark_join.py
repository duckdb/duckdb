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
        df = df.sort(*df.columns)
        res = df.collect()
        expected = [
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
        assert sorted(res) == sorted(expected)

    @pytest.mark.parametrize('how', ['outer', 'fullouter', 'full', 'full_outer'])
    def test_outer_join(self, dataframe_a, dataframe_b, how):
        df = dataframe_a.join(dataframe_b, dataframe_a.emp_dept_id == dataframe_b.dept_id, how)
        df = df.sort(*df.columns)
        res1 = df.collect()
        assert sorted(res1, key=lambda x: x.emp_id or 0) == sorted(
            [
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
            ],
            key=lambda x: x.emp_id or 0,
        )

    @pytest.mark.parametrize('how', ['right', 'rightouter', 'right_outer'])
    def test_right_join(self, dataframe_a, dataframe_b, how):
        df = dataframe_a.join(dataframe_b, dataframe_a.emp_dept_id == dataframe_b.dept_id, how)
        df = df.sort(*df.columns)
        res = df.collect()
        assert sorted(res, key=lambda x: x.emp_id or 0) == sorted(
            [
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
            ],
            key=lambda x: x.emp_id or 0,
        )

    @pytest.mark.parametrize('how', ['semi', 'leftsemi', 'left_semi'])
    def test_semi_join(self, dataframe_a, dataframe_b, how):
        df = dataframe_a.join(dataframe_b, dataframe_a.emp_dept_id == dataframe_b.dept_id, how)
        df = df.sort(*df.columns)
        res = df.collect()
        assert sorted(res) == sorted(
            [
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
                    emp_id=2,
                    name='Rose',
                    superior_emp_id=1,
                    year_joined='2010',
                    emp_dept_id='20',
                    gender='M',
                    salary=4000,
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
                    emp_id=4,
                    name='Jones',
                    superior_emp_id=2,
                    year_joined='2005',
                    emp_dept_id='10',
                    gender='F',
                    salary=2000,
                ),
                Row(
                    emp_id=5,
                    name='Brown',
                    superior_emp_id=2,
                    year_joined='2010',
                    emp_dept_id='40',
                    gender='',
                    salary=-1,
                ),
            ]
        )

    @pytest.mark.parametrize('how', ['anti', 'leftanti', 'left_anti'])
    def test_anti_join(self, dataframe_a, dataframe_b, how):
        df = dataframe_a.join(dataframe_b, dataframe_a.emp_dept_id == dataframe_b.dept_id, how)
        df = df.sort(*df.columns)
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
        df = df.orderBy(*df.columns)
        res = df.collect()
        assert sorted(res, key=lambda x: x.emp_id) == sorted(
            [
                Row(emp_id=2, name='Rose', superior_emp_id=1, superior_emp_name='Smith'),
                Row(emp_id=3, name='Williams', superior_emp_id=1, superior_emp_name='Smith'),
                Row(emp_id=4, name='Jones', superior_emp_id=2, superior_emp_name='Rose'),
                Row(emp_id=5, name='Brown', superior_emp_id=2, superior_emp_name='Rose'),
                Row(emp_id=6, name='Brown', superior_emp_id=2, superior_emp_name='Rose'),
            ],
            key=lambda x: x.emp_id,
        )

    def test_cross_join(self, spark):
        data1 = [(1, "Carol"), (2, "Alice"), (3, "Dave")]
        data2 = [(4, "A"), (5, "B")]
        df1 = spark.createDataFrame(data1, ["age", "name"])
        df2 = spark.createDataFrame(data2, ["id", "rank"])

        df = df1.crossJoin(df2)

        res = df.orderBy("rank", "age").collect()

        assert sorted(res) == sorted(
            [
                Row(age=1, name="Carol", id=4, rank="A"),
                Row(age=2, name="Alice", id=4, rank="A"),
                Row(age=3, name="Dave", id=4, rank="A"),
                Row(age=1, name="Carol", id=5, rank="B"),
                Row(age=2, name="Alice", id=5, rank="B"),
                Row(age=3, name="Dave", id=5, rank="B"),
            ]
        )

    def test_join_with_using_clause(self, spark, dataframe_a):
        dataframe_a = dataframe_a.select('name', 'year_joined')

        df = dataframe_a.alias('df1')
        df2 = dataframe_a.alias('df2')
        res = df.join(df2, ['name', 'year_joined']).sort('name', 'year_joined')
        res = res.collect()
        assert res == [
            Row(name='Brown', year_joined='2010'),
            Row(name='Brown', year_joined='2010'),
            Row(name='Brown', year_joined='2010'),
            Row(name='Brown', year_joined='2010'),
            Row(name='Jones', year_joined='2005'),
            Row(name='Rose', year_joined='2010'),
            Row(name='Smith', year_joined='2018'),
            Row(name='Williams', year_joined='2010'),
        ]

    def test_join_with_common_column(self, spark, dataframe_a):
        dataframe_a = dataframe_a.select('name', 'year_joined')

        df = dataframe_a.alias('df1')
        df2 = dataframe_a.alias('df2')
        res = df.join(df2, df.name == df2.name).sort('df1.name')
        res = res.collect()
        assert (
            str(res)
            == "[Row(name='Brown', year_joined='2010', name='Brown', year_joined='2010'), Row(name='Brown', year_joined='2010', name='Brown', year_joined='2010'), Row(name='Brown', year_joined='2010', name='Brown', year_joined='2010'), Row(name='Brown', year_joined='2010', name='Brown', year_joined='2010'), Row(name='Jones', year_joined='2005', name='Jones', year_joined='2005'), Row(name='Rose', year_joined='2010', name='Rose', year_joined='2010'), Row(name='Smith', year_joined='2018', name='Smith', year_joined='2018'), Row(name='Williams', year_joined='2010', name='Williams', year_joined='2010')]"
        )

    @pytest.mark.xfail(condition=True, reason="Selecting from a duplicate binding causes an error")
    def test_join_on_joined_data_error(self, spark):
        df = spark.createDataFrame([(2, "Alice"), (5, "Bob")]).toDF("age", "name")
        df2 = spark.createDataFrame([Row(height=80, name="Tom"), Row(height=85, name="Bob")], ["height", "name"])

        first_join = df.join(df2, "name")
        second_join = df.join(df2, "name")
        third_join = first_join.join(second_join, "name")

        third_join.show()

    def test_join_on_column_name_stored_in_variable(self, spark):
        df = spark.createDataFrame([(2, "Alice"), (5, "Bob")]).toDF("age", "name")
        df2 = spark.createDataFrame([Row(height=80, name="Tom"), Row(height=85, name="Bob")], ["height", "name"])

        column = "name"
        col1 = df[column]
        col2 = df2[column]
        res = df.join(df2, col1 == col2)
        res = res.collect()
        assert str(res) == "[Row(age=5, name='Bob', height=85, name='Bob')]"
