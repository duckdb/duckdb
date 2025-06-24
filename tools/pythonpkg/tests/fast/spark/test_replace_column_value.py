import pytest

_ = pytest.importorskip("duckdb.experimental.spark")
from spark_namespace.sql.types import Row


class TestReplaceValue(object):
    # https://sparkbyexamples.com/pyspark/pyspark-replace-column-values/?expand_article=1
    def test_replace_value(self, spark):
        address = [(1, "14851 Jeffrey Rd", "DE"), (2, "43421 Margarita St", "NY"), (3, "13111 Siemon Ave", "CA")]
        df = spark.createDataFrame(address, ["id", "address", "state"])

        # Replace part of string with another string
        from spark_namespace.sql.functions import regexp_replace

        df2 = df.withColumn('address', regexp_replace('address', 'Rd', 'Road'))

        # Replace string column value conditionally
        from spark_namespace.sql.functions import when

        res = df2.collect()
        print(res)
        df2 = df.withColumn(
            'address',
            when(df.address.endswith('Rd'), regexp_replace(df.address, 'Rd', 'Road'))
            .when(df.address.endswith('St'), regexp_replace(df.address, 'St', 'Street'))
            .when(df.address.endswith('Ave'), regexp_replace(df.address, 'Ave', 'Avenue'))
            .otherwise(df.address),
        )
        res = df2.collect()
        print(res)
        expected = [
            Row(id=1, address='14851 Jeffrey Road', state='DE'),
            Row(id=2, address='43421 Margarita Street', state='NY'),
            Row(id=3, address='13111 Siemon Avenue', state='CA'),
        ]
        print(expected)
        assert res == expected
        # Replace all substrings of the specified string value that match regexp with rep.
        df3 = spark.createDataFrame([('100-200',)], ['str'])
        res = df3.select(regexp_replace('str', r'(\d+)', '--').alias('d')).collect()
        expected = [Row(d='-----')]
        print(expected)
        assert res == expected
