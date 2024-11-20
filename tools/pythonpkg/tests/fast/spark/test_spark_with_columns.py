import pytest

_ = pytest.importorskip("duckdb.experimental.spark")


from spark_namespace.sql.functions import col, lit
from spark_namespace import USE_ACTUAL_SPARK


class TestWithColumns:
    def test_with_columns(self, spark):
        data = [
            ('James', '', 'Smith', '1991-04-01', 'M', 3000),
            ('Michael', 'Rose', '', '2000-05-19', 'M', 4000),
            ('Robert', '', 'Williams', '1978-09-05', 'M', 4000),
            ('Maria', 'Anne', 'Jones', '1967-12-01', 'F', 4000),
            ('Jen', 'Mary', 'Brown', '1980-02-17', 'F', -1),
        ]

        columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]
        df = spark.createDataFrame(data=data, schema=columns)
        assert df.schema['salary'].dataType.typeName() == ('long' if USE_ACTUAL_SPARK else 'integer')

        # The type of 'salary' has been cast to Bigint
        new_df = df.withColumns({"salary": col("salary").cast("BIGINT")})
        assert new_df.schema['salary'].dataType.typeName() == 'long'

        # Replace the 'salary' column with '(salary * 100)' and add a new column
        # from an existing column
        df2 = df.withColumns({"salary": col("salary") * 100, "CopiedColumn": col("salary") * -1})
        res = df2.collect()
        assert res[0].salary == 300_000
        assert res[0].CopiedColumn == -3000

        df2 = df.withColumns({"Country": lit("USA")})
        res = df2.collect()
        assert res[0].Country == 'USA'

        df2 = df.withColumns({"Country": lit("USA")}).withColumns({"anotherColumn": lit("anotherValue")})
        res = df2.collect()
        assert res[0].Country == 'USA'
        assert res[1].anotherColumn == 'anotherValue'

        df2 = df.drop("salary")
        assert 'salary' not in df2.columns
