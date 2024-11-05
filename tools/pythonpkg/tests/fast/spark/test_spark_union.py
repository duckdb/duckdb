import platform
import pytest

_ = pytest.importorskip("duckdb.experimental.spark")

from spark_namespace.sql.types import Row
from spark_namespace.sql.functions import col


@pytest.fixture
def df(spark):
    simpleData = [
        ("James", "Sales", "NY", 90000, 34, 10000),
        ("Michael", "Sales", "NY", 86000, 56, 20000),
        ("Robert", "Sales", "CA", 81000, 30, 23000),
        ("Maria", "Finance", "CA", 90000, 24, 23000),
    ]

    columns = ["employee_name", "department", "state", "salary", "age", "bonus"]
    dataframe = spark.createDataFrame(data=simpleData, schema=columns)
    yield dataframe


@pytest.fixture
def df2(spark):
    simpleData2 = [
        ("James", "Sales", "NY", 90000, 34, 10000),
        ("Maria", "Finance", "CA", 90000, 24, 23000),
        ("Jen", "Finance", "NY", 79000, 53, 15000),
        ("Jeff", "Marketing", "CA", 80000, 25, 18000),
        ("Kumar", "Marketing", "NY", 91000, 50, 21000),
    ]
    columns2 = ["employee_name", "department", "state", "salary", "age", "bonus"]
    dataframe = spark.createDataFrame(data=simpleData2, schema=columns2)
    yield dataframe


class TestDataFrameUnion(object):
    def test_merge_with_union(self, df, df2):
        unionDF = df.union(df2)
        res = unionDF.collect()
        assert res == [
            Row(employee_name='James', department='Sales', state='NY', salary=90000, age=34, bonus=10000),
            Row(employee_name='Michael', department='Sales', state='NY', salary=86000, age=56, bonus=20000),
            Row(employee_name='Robert', department='Sales', state='CA', salary=81000, age=30, bonus=23000),
            Row(employee_name='Maria', department='Finance', state='CA', salary=90000, age=24, bonus=23000),
            Row(employee_name='James', department='Sales', state='NY', salary=90000, age=34, bonus=10000),
            Row(employee_name='Maria', department='Finance', state='CA', salary=90000, age=24, bonus=23000),
            Row(employee_name='Jen', department='Finance', state='NY', salary=79000, age=53, bonus=15000),
            Row(employee_name='Jeff', department='Marketing', state='CA', salary=80000, age=25, bonus=18000),
            Row(employee_name='Kumar', department='Marketing', state='NY', salary=91000, age=50, bonus=21000),
        ]
        unionDF = df.unionAll(df2)
        res2 = unionDF.collect()
        assert res == res2

    @pytest.mark.xfail(condition=platform.system() == "Emscripten", reason="Broken on Pyodide")
    def test_merge_without_duplicates(self, df, df2):
        # 'sort' has been added to make the result deterministic
        disDF = df.union(df2).distinct().sort(col("employee_name"))
        res = disDF.collect()
        assert res == [
            Row(employee_name='James', department='Sales', state='NY', salary=90000, age=34, bonus=10000),
            Row(employee_name='Jeff', department='Marketing', state='CA', salary=80000, age=25, bonus=18000),
            Row(employee_name='Jen', department='Finance', state='NY', salary=79000, age=53, bonus=15000),
            Row(employee_name='Kumar', department='Marketing', state='NY', salary=91000, age=50, bonus=21000),
            Row(employee_name='Maria', department='Finance', state='CA', salary=90000, age=24, bonus=23000),
            Row(employee_name='Michael', department='Sales', state='NY', salary=86000, age=56, bonus=20000),
            Row(employee_name='Robert', department='Sales', state='CA', salary=81000, age=30, bonus=23000),
        ]
