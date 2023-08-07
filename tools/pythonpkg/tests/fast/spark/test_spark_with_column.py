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
from pyduckdb.spark.sql.functions import col, struct, when
import duckdb
import re


class TestWithColumn(object):
    def test_with_column(self, spark):
        data = [
            ('James', '', 'Smith', '1991-04-01', 'M', 3000),
            ('Michael', 'Rose', '', '2000-05-19', 'M', 4000),
            ('Robert', '', 'Williams', '1978-09-05', 'M', 4000),
            ('Maria', 'Anne', 'Jones', '1967-12-01', 'F', 4000),
            ('Jen', 'Mary', 'Brown', '1980-02-17', 'F', -1),
        ]

        columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]
        df = spark.createDataFrame(data=data, schema=columns)
        assert df.schema['salary'].dataType.typeName() == 'short'

        # The type of 'salary' has been cast to Integer
        new_df = df.withColumn("salary", col("salary").cast("Integer"))
        assert new_df.schema['salary'].dataType.typeName() == 'integer'

        df.withColumn("salary", col("salary") * 100).show()
