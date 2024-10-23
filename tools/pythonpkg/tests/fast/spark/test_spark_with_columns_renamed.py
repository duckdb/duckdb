import re
import pytest

_ = pytest.importorskip("duckdb.experimental.spark")


class TestWithColumnsRenamed(object):
    def test_with_columns_renamed(self, spark):
        dataDF = [
            (('James', '', 'Smith'), '1991-04-01', 'M', 3000),
            (('Michael', 'Rose', ''), '2000-05-19', 'M', 4000),
            (('Robert', '', 'Williams'), '1978-09-05', 'M', 4000),
            (('Maria', 'Anne', 'Jones'), '1967-12-01', 'F', 4000),
            (('Jen', 'Mary', 'Brown'), '1980-02-17', 'F', -1),
        ]
        from ...spark_namespace.sql.types import StructType, StructField, StringType, IntegerType

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
        assert 'dob' not in df2
        assert 'salary' not in df2
        assert 'DateOfBirth' in df2
        assert 'salary_amount' in df2

        df2 = df.withColumnsRenamed({"name": "full name"})
        assert 'name' not in df2
        assert 'full name' in df2
        assert 'firstname' in df2.schema['full name'].dataType

        with pytest.raises(ValueError, match=re.escape("DataFrame does not contain column(s): unknown col")):
            df2.withColumnsRenamed({"unknown col": "new name", "full name": "name"})
