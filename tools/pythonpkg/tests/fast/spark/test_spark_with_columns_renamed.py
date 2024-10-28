import re
import pytest

_ = pytest.importorskip("duckdb.experimental.spark")

from spark_namespace import USE_ACTUAL_SPARK


class TestWithColumnsRenamed(object):
    def test_with_columns_renamed(self, spark):
        dataDF = [
            (('James', '', 'Smith'), '1991-04-01', 'M', 3000),
            (('Michael', 'Rose', ''), '2000-05-19', 'M', 4000),
            (('Robert', '', 'Williams'), '1978-09-05', 'M', 4000),
            (('Maria', 'Anne', 'Jones'), '1967-12-01', 'F', 4000),
            (('Jen', 'Mary', 'Brown'), '1980-02-17', 'F', -1),
        ]
        from spark_namespace.sql.types import StructType, StructField, StringType, IntegerType

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
                StructField('dob', StringType(), True),
                StructField('gender', StringType(), True),
                StructField('salary', IntegerType(), True),
            ]
        )

        df = spark.createDataFrame(data=dataDF, schema=schema)

        df2 = df.withColumnsRenamed({"dob": "DateOfBirth", "salary": "salary_amount"})
        assert 'dob' not in df2.columns
        assert 'salary' not in df2.columns
        assert 'DateOfBirth' in df2.columns
        assert 'salary_amount' in df2.columns

        df2 = df.withColumnsRenamed({"name": "full name"})
        assert 'name' not in df2.columns
        assert 'full name' in df2.columns
        assert 'firstname' in df2.schema['full name'].dataType.fieldNames()

        # PySpark does not raise an error. This is a convenience we provide in DuckDB.
        if not USE_ACTUAL_SPARK:
            with pytest.raises(ValueError, match=re.escape("DataFrame does not contain column(s): unknown col")):
                df2.withColumnsRenamed({"unknown col": "new name", "full name": "name"})
