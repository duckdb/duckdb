import pytest

_ = pytest.importorskip("duckdb.experimental.spark")

from duckdb.experimental.spark.sql.column import Column
from duckdb.experimental.spark.sql.functions import struct
from duckdb.experimental.spark.sql.types import Row

import duckdb


class TestSparkColumn(object):
    def test_struct_column(self, spark):
        df = spark.createDataFrame([Row(a=1, b=2, c=3, d=4)])

        df = df.withColumn('struct', struct(df.col0, df.col1))
        assert 'struct' in df
        new_col = df.schema['struct']
        assert 'col0' in new_col.dataType
        assert 'col1' in new_col.dataType

        with pytest.raises(TypeError, match="argument 'col' should be of type Column"):
            df = df.withColumn('struct', 'yes')
