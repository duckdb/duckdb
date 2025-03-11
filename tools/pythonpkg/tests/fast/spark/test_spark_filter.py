import pytest

_ = pytest.importorskip("duckdb.experimental.spark")

from spark_namespace import USE_ACTUAL_SPARK
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
from spark_namespace.errors import PySparkTypeError
import duckdb
import re


class TestDataFrameFilter(object):
    def test_dataframe_filter(self, spark):
        data = [
            (("James", "", "Smith"), ["Java", "Scala", "C++"], "OH", "M"),
            (("Anna", "Rose", ""), ["Spark", "Java", "C++"], "CA", "F"),
            (("Julia", "", "Williams"), ["CSharp", "VB"], "OH", "F"),
            (("Maria", "Anne", "Jones"), ["CSharp", "VB"], "NY", "M"),
            (("Jen", "Mary", "Brown"), ["CSharp", "VB"], "NY", "M"),
            (("Mike", "Mary", "Williams"), ["Python", "VB"], "OH", "M"),
        ]

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
                StructField('languages', ArrayType(StringType()), True),
                StructField('state', StringType(), True),
                StructField('gender', StringType(), True),
            ]
        )

        df = spark.createDataFrame(data=data, schema=schema)

        # --- Tests ---

        # Using equals condition
        df2 = df.filter(df.state == "OH")
        res = df2.collect()
        assert res[0].state == 'OH'

        # not equals condition
        df2 = df.filter(df.state != "OH")
        df2 = df.filter(~(df.state == "OH"))
        res = df2.collect()
        for item in res:
            assert item.state == 'NY' or item.state == 'CA'

        df2 = df.filter(col("state") == "OH")
        res = df2.collect()
        assert res[0].state == 'OH'

        df2 = df.filter("gender == 'M'")
        res = df2.collect()
        assert res[0].gender == 'M'

        df2 = df.filter("gender != 'M'")
        res = df2.collect()
        assert res[0].gender == 'F'

        df2 = df.filter("gender <> 'M'")
        res = df2.collect()
        assert res[0].gender == 'F'

        # Filter multiple condition
        df2 = df.filter((df.state == "OH") & (df.gender == "M"))
        res = df2.collect()
        assert len(res) == 2
        for item in res:
            assert item.gender == 'M' and item.state == 'OH'

        # Filter IS IN List values
        li = ["OH", "NY"]
        df2 = df.filter(df.state.isin(li))
        res = df2.collect()
        for item in res:
            assert item.state == 'OH' or item.state == 'NY'

        # Filter NOT IS IN List values
        # These show all records with NY (NY is not part of the list)
        df2 = df.filter(~df.state.isin(li))
        res = df2.collect()
        for item in res:
            assert item.state != 'OH' and item.state != 'NY'

        df2 = df.filter(df.state.isin(li) == False)
        res2 = df2.collect()
        assert res2 == res

        # Using startswith
        df2 = df.filter(df.state.startswith("N"))
        res = df2.collect()
        for item in res:
            assert item.state == 'NY'

        # using endswith
        df2 = df.filter(df.state.endswith("H"))
        res = df2.collect()
        for item in res:
            assert item.state == 'OH'

        # contains
        df2 = df.filter(df.state.contains("H"))
        res = df2.collect()
        for item in res:
            assert item.state == 'OH'

        data2 = [(2, "Michael Rose"), (3, "Robert Williams"), (4, "Rames Rose"), (5, "Rames rose")]
        df2 = spark.createDataFrame(data=data2, schema=["id", "name"])

        # like - SQL LIKE pattern
        df3 = df2.filter(df2.name.like("%rose%"))
        res = df3.collect()
        assert res == [Row(id=5, name='Rames rose')]

        # rlike - SQL RLIKE pattern (LIKE with Regex)
        # This check case insensitive
        df3 = df2.filter(df2.name.rlike("(?i)^*rose$"))
        res = df3.collect()
        assert res == [Row(id=2, name='Michael Rose'), Row(id=4, name='Rames Rose'), Row(id=5, name='Rames rose')]

        df2 = df.filter(array_contains(df.languages, "Java"))
        res = df2.collect()

        james_name = {'firstname': 'James', 'middlename': '', 'lastname': 'Smith'}
        anna_name = {'firstname': 'Anna', 'middlename': 'Rose', 'lastname': ''}
        if USE_ACTUAL_SPARK:
            james_name = Row(**james_name)
            anna_name = Row(**anna_name)
        assert res == [
            Row(
                name=james_name,
                languages=['Java', 'Scala', 'C++'],
                state='OH',
                gender='M',
            ),
            Row(
                name=anna_name,
                languages=['Spark', 'Java', 'C++'],
                state='CA',
                gender='F',
            ),
        ]

        df2 = df.filter(df.name.lastname == "Williams")
        res = df2.collect()
        julia_name = {'firstname': 'Julia', 'middlename': '', 'lastname': 'Williams'}
        mike_name = {'firstname': 'Mike', 'middlename': 'Mary', 'lastname': 'Williams'}
        if USE_ACTUAL_SPARK:
            julia_name = Row(**julia_name)
            mike_name = Row(**mike_name)
        assert res == [
            Row(
                name=julia_name,
                languages=['CSharp', 'VB'],
                state='OH',
                gender='F',
            ),
            Row(
                name=mike_name,
                languages=['Python', 'VB'],
                state='OH',
                gender='M',
            ),
        ]

    def test_invalid_condition_type(self, spark):
        df = spark.createDataFrame([(1, "A")], ["A", "B"])

        with pytest.raises(PySparkTypeError):
            df = df.filter(dict(a=1))
