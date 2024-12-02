import pytest

_ = pytest.importorskip("duckdb.experimental.spark")

from spark_namespace import USE_ACTUAL_SPARK
from spark_namespace.sql.types import Row


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
        from spark_namespace.sql.functions import col, when

        df2 = df.withColumn("name", when(col("name") == "", None).otherwise(col("name")))
        assert df2.columns == ['name', 'state']
        res = df2.select('name').collect()
        assert res == [Row(name=None), Row(name='Julia'), Row(name='Robert'), Row(name=None)]

        # Replace state + name
        from spark_namespace.sql.functions import col, when

        df2 = df.select([when(col(c) == "", None).otherwise(col(c)).alias(c) for c in df.columns])
        assert df2.columns == ['name', 'state']
        key_f = lambda x: x.name or x.state
        res = df2.sort("name", "state").collect()
        expected_res = [
            Row(name=None, state='CA'),
            Row(name=None, state='NJ'),
            Row(name='Julia', state=None),
            Row(name='Robert', state=None),
        ]
        assert res == expected_res

        # On selection of columns
        # Replace empty string with None on selected columns
        from spark_namespace.sql.functions import col, when

        replaceCols = ["state"]
        df2 = df.select([when(col(c) == "", None).otherwise(col(c)).alias(c) for c in replaceCols]).sort(col('state'))
        assert df2.columns == ['state']

        key_f = lambda x: x.state or ""
        res = df2.collect()
        assert sorted(res, key=key_f) == sorted(
            [
                Row(state='CA'),
                Row(state='NJ'),
                Row(state=None),
                Row(state=None),
            ],
            key=key_f,
        )
