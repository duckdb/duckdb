import pytest

_ = pytest.importorskip("pyduckdb.spark")
from pyduckdb.spark.sql.types import Row


# https://sparkbyexamples.com/pyspark/pyspark-replace-column-values/?expand_article=1
class TestReplaceValue(object):
    def test_replace_value(self, spark):
        address = [(1, "14851 Jeffrey Rd", "DE"), (2, "43421 Margarita St", "NY"), (3, "13111 Siemon Ave", "CA")]
        df = spark.createDataFrame(address, ["id", "address", "state"])

        # Replace part of string with another string
        from pyduckdb.spark.sql.functions import regexp_replace

        df2 = df.withColumn('address', regexp_replace('address', 'Rd', 'Road'))
        df2.show()

        # Replace string column value conditionally
        from pyduckdb.spark.sql.functions import when

        df2 = df.withColumn(
            'address',
            when(df.address.endswith('Rd'), regexp_replace(df.address, 'Rd', 'Road'))
            .when(df.address.endswith('St'), regexp_replace(df.address, 'St', 'Street'))
            .when(df.address.endswith('Ave'), regexp_replace(df.address, 'Ave', 'Avenue'))
            .otherwise(df.address),
        )
        df2.show()

        # NOTE: We don't have support for the 'translate' function yet
        # awaiting https://github.com/duckdb/duckdb/pull/5202

        ##Using translate to replace character by character
        # from pyduckdb.spark.sql.functions import translate
        # df2 = df.withColumn('address', translate('address', '123', 'ABC'))

        ##Replace column with another column
        # from pyduckdb.spark.sql.functions import expr
        # df = spark.createDataFrame(
        # [("ABCDE_XYZ", "XYZ","FGH")],
        # 	("col1", "col2","col3")
        # )
        # df.withColumn("new_column",
        # 			expr("regexp_replace(col1, col2, col3)")
        # 			.alias("replaced_value")
        # 			).show()

        # Overlay
        df = spark.createDataFrame([("ABCDE_XYZ", "FGH")], ("col1", "col2"))
        from pyduckdb.spark.sql.functions import overlay

        df.select(overlay("col1", "col2", 7).alias("overlayed")).show()
