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
from duckdb.experimental.spark.sql.functions import col, struct, when
import duckdb
import re

from duckdb.experimental.spark.errors import PySparkValueError, PySparkTypeError


class TestDataFrame(object):
    def test_dataframe_from_list_of_tuples(self, spark):
        # Valid
        address = [(1, "14851 Jeffrey Rd", "DE"), (2, "43421 Margarita St", "NY"), (3, "13111 Siemon Ave", "CA")]
        df = spark.createDataFrame(address, ["id", "address", "state"])
        res = df.collect()
        assert res == [
            Row(id=1, address='14851 Jeffrey Rd', state='DE'),
            Row(id=2, address='43421 Margarita St', state='NY'),
            Row(id=3, address='13111 Siemon Ave', state='CA'),
        ]

        # Tuples of different sizes
        address = [(1, "14851 Jeffrey Rd", "DE"), (2, "43421 Margarita St", "NY"), (3, "13111 Siemon Ave")]
        with pytest.raises(PySparkTypeError, match="LENGTH_SHOULD_BE_THE_SAME"):
            df = spark.createDataFrame(address, ["id", "address", "state"])

        # Dataframe instead of list
        with pytest.raises(PySparkTypeError, match="SHOULD_NOT_DATAFRAME"):
            df = spark.createDataFrame(df, ["id", "address", "state"])

        # Not a list
        with pytest.raises(TypeError, match="not iterable"):
            df = spark.createDataFrame(5, ["id", "address", "test"])

        # Empty list
        df = spark.createDataFrame([], ["id", "address", "test"])
        res = df.collect()
        assert res == []

        # Duplicate column names
        address = [(1, "14851 Jeffrey Rd", "DE"), (2, "43421 Margarita St", "NY"), (3, "13111 Siemon Ave", "DE")]
        df = spark.createDataFrame(address, ["id", "address", "id"])
        res = df.collect()
        assert (
            str(res)
            == "[Row(id=1, address='14851 Jeffrey Rd', id='DE'), Row(id=2, address='43421 Margarita St', id='NY'), Row(id=3, address='13111 Siemon Ave', id='DE')]"
        )

        # Not enough column names
        with pytest.raises(PySparkValueError, match="number of columns in the DataFrame don't match"):
            df = spark.createDataFrame(address, ["id", "address"])

        # Empty column names list
        # Columns are filled in with default names
        # TODO: check against Spark behavior
        df = spark.createDataFrame(address, [])
        res = df.collect()
        assert res == [
            Row(col0=1, col1='14851 Jeffrey Rd', col2='DE'),
            Row(col0=2, col1='43421 Margarita St', col2='NY'),
            Row(col0=3, col1='13111 Siemon Ave', col2='DE'),
        ]

        # Too many column names
        with pytest.raises(PySparkValueError, match="number of columns in the DataFrame don't match"):
            df = spark.createDataFrame(address, ["id", "address", "one", "two", "three"])

        # Column names is not a list (but is iterable)
        df = spark.createDataFrame(address, {'a': 5, 'b': 6, 'c': 42})
        res = df.collect()
        assert res == [
            Row(a=1, b='14851 Jeffrey Rd', c='DE'),
            Row(a=2, b='43421 Margarita St', c='NY'),
            Row(a=3, b='13111 Siemon Ave', c='DE'),
        ]

        # Column names is not a list (string, becomes a single column name)
        with pytest.raises(PySparkValueError, match="number of columns in the DataFrame don't match"):
            df = spark.createDataFrame(address, 'a')

        with pytest.raises(TypeError, match="must be an iterable, not int"):
            df = spark.createDataFrame(address, 5)

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
        assert str(res) == '[Row(a=42), Row(a=21)]'

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
        from duckdb.experimental.spark.sql.types import StructType, StructField, StringType, IntegerType

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

    def test_df_columns(self, spark):
        from duckdb.experimental.spark.sql.functions import col, struct, when

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
        updatedDF = df2.withColumn(
            "OtherInfo",
            struct(
                col("id").alias("identifier"),
                col("gender").alias("gender"),
                col("salary").alias("salary"),
                when(col("salary").cast(IntegerType()) < 2000, "Low")
                .when(col("salary").cast(IntegerType()) < 4000, "Medium")
                .otherwise("High")
                .alias("Salary_Grade"),
            ),
        ).drop("id", "gender", "salary")

        assert 'OtherInfo' in updatedDF

    def test_array_and_map_type(self, spark):
        """Array & Map"""

        arrayStructureSchema = StructType(
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
                StructField('hobbies', ArrayType(StringType()), True),
                StructField('properties', MapType(StringType(), StringType()), True),
            ]
        )
