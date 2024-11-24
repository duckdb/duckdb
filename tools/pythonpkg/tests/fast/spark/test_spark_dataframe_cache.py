import pytest

_ = pytest.importorskip("duckdb.experimental.spark")

import spark_namespace.sql.functions as F


class TestDataFrameCache(object):

    def test_cache(self, spark):
        df_base = spark.range(0, 10).select(F.rand().alias("rand"))
        df_cached = df_base.cache()

        assert df_base.union(df_base).union(df_base).distinct().count() > 10
        assert df_cached.union(df_cached).union(df_cached).distinct().count() == 10
