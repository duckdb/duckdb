import pytest

_ = pytest.importorskip("duckdb.experimental.spark")
from duckdb.experimental.spark.sql.types import Row


# https://sparkbyexamples.com/pyspark/pyspark-replace-empty-value-with-none-on-dataframe-2/?expand_article=1
class TestReplaceEmpty(object):
    def test_replace_empty(self, spark):
        # Create the dataframe
        data = [("", "CA"), ("Julia", ""), ("Robert", ""), ("", "NJ")]
        df = spark.createDataFrame(data, ["name", "state"])
        res = df.select('name').collect()
        assert res == [Row(name=''), Row(name='Julia'), Row(name='Robert'), Row(name='')]
        res = df.select('state').collect()
        assert res == [Row(state='CA'), Row(state=''), Row(state=''), Row(state='NJ')]

        # Replace name
        # CASE WHEN "name" == '' THEN NULL ELSE "name" END
        from duckdb.experimental.spark.sql.functions import col, when

        df2 = df.withColumn("name", when(col("name") == "", None).otherwise(col("name")))
        assert df2.columns == ['name', 'state']
        res = df2.select('name').collect()
        assert res == [Row(name=None), Row(name='Julia'), Row(name='Robert'), Row(name=None)]

        # Replace state + name
        from duckdb.experimental.spark.sql.functions import col, when

        df2 = df.select([when(col(c) == "", None).otherwise(col(c)).alias(c) for c in df.columns])
        assert df2.columns == ['name', 'state']
        res = df2.collect()
        assert res == [
            Row(name=None, state='CA'),
            Row(name='Julia', state=None),
            Row(name='Robert', state=None),
            Row(name=None, state='NJ'),
        ]

        # On selection of columns
        # Replace empty string with None on selected columns
        from duckdb.experimental.spark.sql.functions import col, when

        replaceCols = ["state"]
        df2 = df.select([when(col(c) == "", None).otherwise(col(c)).alias(c) for c in replaceCols]).sort(col('state'))
        assert df2.columns == ['state']
        res = df2.collect()
        assert res == [
            Row(state='CA'),
            Row(state='NJ'),
            Row(state=None),
            Row(state=None),
        ]
