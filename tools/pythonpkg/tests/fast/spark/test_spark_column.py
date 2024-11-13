import pytest

_ = pytest.importorskip("duckdb.experimental.spark")

from spark_namespace import USE_ACTUAL_SPARK
from spark_namespace.sql.column import Column
from spark_namespace.sql.functions import struct, array
from spark_namespace.sql.types import Row
from spark_namespace.errors import PySparkTypeError

import duckdb
import re


class TestSparkColumn(object):
    def test_struct_column(self, spark):
        df = spark.createDataFrame([Row(a=1, b=2, c=3, d=4)])

        # FIXME: column names should be set explicitly using the Row, rather than letting duckdb assign defaults (col0, col1, etc..)
        if USE_ACTUAL_SPARK:
            df = df.withColumn('struct', struct(df.a, df.b))
        else:
            df = df.withColumn('struct', struct(df.col0, df.col1))
            assert 'struct' in df
            new_col = df.schema['struct']

        if USE_ACTUAL_SPARK:
            assert 'a' in df.schema['struct'].dataType.fieldNames()
            assert 'b' in df.schema['struct'].dataType.fieldNames()
        else:
            assert 'col0' in new_col.dataType
            assert 'col1' in new_col.dataType

        with pytest.raises(
            PySparkTypeError, match=re.escape("[NOT_COLUMN] Argument `col` should be a Column, got str.")
        ):
            df = df.withColumn('struct', 'yes')

    def test_array_column(self, spark):
        df = spark.createDataFrame([Row(a=1, b=2, c=3, d=4)], ['a', 'b', 'c', 'd'])

        df2 = df.select(
            array(df["a"], df["b"]).alias("array"),
            array(df["a"]).alias("array_single"),
            array("b", "c", "d").alias("array_str"),
        )
        res = df2.collect()

        assert res[0].array == [1, 2]
        assert res[0].array_single == [1]
        assert res[0].array_str == [2, 3, 4]
