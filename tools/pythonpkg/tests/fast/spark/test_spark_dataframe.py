import pytest

_ = pytest.importorskip("pyduckdb.spark")
from pyduckdb.spark.sql.types import Row
from pyduckdb.spark.sql.types import LongType, StructType, BooleanType, StructField, StringType, IntegerType
import duckdb
import re


class TestDataFrame(object):
    def test_dataframe(self, spark):
        # Create DataFrame
        df = spark.createDataFrame([("Scala", 25000), ("Spark", 35000), ("PHP", 21000)])
        res = df.collect()
        assert res == [Row(col0='Scala', col1=25000), Row(col0='Spark', col1=35000), Row(col0='PHP', col1=21000)]

    def test_writing_to_table(self, spark):
        # Create Hive table & query it.
        spark.sql(
            """
			create table sample_table("_1" bool, "_2" integer)
		"""
        )
        spark.sql('insert into sample_table VALUES (True, 42)')
        spark.table("sample_table").write.saveAsTable("sample_hive_table")
        df3 = spark.sql("SELECT _1,_2 FROM sample_hive_table")
        res = df3.collect()
        assert res == [Row(_1=True, _2=42)]
        schema = df3.schema
        assert schema == StructType([StructField('_1', BooleanType(), True), StructField('_2', IntegerType(), True)])

    def test_dataframe_collect(self, spark):
        df = spark.createDataFrame([(42,), (21,)]).toDF('a')
        res = df.collect()
        assert res == [Row(a=42), Row(a=21)]

    def test_dataframe_from_rows(self, spark):
        columns = ["language", "users_count"]
        data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]

        rowData = map(lambda x: Row(*x), data)
        df = spark.createDataFrame(rowData, columns)
        res = df.collect()
        assert res == [
            Row(language='Java', users_count='20000'),
            Row(language='Python', users_count='100000'),
            Row(language='Scala', users_count='3000'),
        ]

    def test_empty_df(self, spark):
        schema = StructType(
            [
                StructField('firstname', StringType(), True),
                StructField('middlename', StringType(), True),
                StructField('lastname', StringType(), True),
            ]
        )
        df = spark.createDataFrame([], schema=schema)
        res = df.collect()
        # TODO: assert that the types and column names are correct
        assert res == []

    def test_df_from_pandas(self, spark):
        import pandas as pd

        df = spark.createDataFrame(pd.DataFrame({'a': [42, 21], 'b': [True, False]}))
        res = df.collect()
        assert res == [Row(a=42, b=True), Row(a=21, b=False)]

    def test_df_from_struct_type(self, spark):
        schema = StructType([StructField('a', LongType()), StructField('b', BooleanType())])
        df = spark.createDataFrame([(42, True), (21, False)], schema)
        res = df.collect()
        assert res == [Row(a=42, b=True), Row(a=21, b=False)]

    def test_df_from_name_list(self, spark):
        df = spark.createDataFrame([(42, True), (21, False)], ['a', 'b'])
        res = df.collect()
        assert res == [Row(a=42, b=True), Row(a=21, b=False)]

    def test_df_creation_coverage(self, spark):
        from pyduckdb.spark.sql.types import StructType, StructField, StringType, IntegerType

        data2 = [
            ("James", "", "Smith", "36636", "M", 3000),
            ("Michael", "Rose", "", "40288", "M", 4000),
            ("Robert", "", "Williams", "42114", "M", 4000),
            ("Maria", "Anne", "Jones", "39192", "F", 4000),
            ("Jen", "Mary", "Brown", "", "F", -1),
        ]

        schema = StructType(
            [
                StructField("firstname", StringType(), True),
                StructField("middlename", StringType(), True),
                StructField("lastname", StringType(), True),
                StructField("id", StringType(), True),
                StructField("gender", StringType(), True),
                StructField("salary", IntegerType(), True),
            ]
        )

        df = spark.createDataFrame(data=data2, schema=schema)
        res = df.collect()
        assert res == [
            Row(firstname='James', middlename='', lastname='Smith', id='36636', gender='M', salary=3000),
            Row(firstname='Michael', middlename='Rose', lastname='', id='40288', gender='M', salary=4000),
            Row(firstname='Robert', middlename='', lastname='Williams', id='42114', gender='M', salary=4000),
            Row(firstname='Maria', middlename='Anne', lastname='Jones', id='39192', gender='F', salary=4000),
            Row(firstname='Jen', middlename='Mary', lastname='Brown', id='', gender='F', salary=-1),
        ]

    def test_df_nested_struct(self, spark):
        structureData = [
            (("James", "", "Smith"), "36636", "M", 3100),
            (("Michael", "Rose", ""), "40288", "M", 4300),
            (("Robert", "", "Williams"), "42114", "M", 1400),
            (("Maria", "Anne", "Jones"), "39192", "F", 5500),
            (("Jen", "Mary", "Brown"), "", "F", -1),
        ]
        structureSchema = StructType(
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
                StructField('id', StringType(), True),
                StructField('gender', StringType(), True),
                StructField('salary', IntegerType(), True),
            ]
        )

        df2 = spark.createDataFrame(data=structureData, schema=structureSchema)
        res = df2.collect()
        assert res == [
            Row(
                name={'firstname': 'James', 'middlename': '', 'lastname': 'Smith'}, id='36636', gender='M', salary=3100
            ),
            Row(
                name={'firstname': 'Michael', 'middlename': 'Rose', 'lastname': ''}, id='40288', gender='M', salary=4300
            ),
            Row(
                name={'firstname': 'Robert', 'middlename': '', 'lastname': 'Williams'},
                id='42114',
                gender='M',
                salary=1400,
            ),
            Row(
                name={'firstname': 'Maria', 'middlename': 'Anne', 'lastname': 'Jones'},
                id='39192',
                gender='F',
                salary=5500,
            ),
            Row(name={'firstname': 'Jen', 'middlename': 'Mary', 'lastname': 'Brown'}, id='', gender='F', salary=-1),
        ]
        schema = df2.schema
        assert schema == StructType(
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
                    True,
                ),
                StructField('id', StringType(), True),
                StructField('gender', StringType(), True),
                StructField('salary', IntegerType(), True),
            ]
        )
