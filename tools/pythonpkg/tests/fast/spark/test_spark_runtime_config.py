import pytest

_ = pytest.importorskip("duckdb.experimental.spark")


class TestSparkRuntimeConfig(object):

    def test_spark_runtime_config_get_set(self, spark):
        spark.conf.set("threads", "1")
        assert spark.conf.get("threads") == "1"

        spark.conf.set("threads", "2")
        assert spark.conf.get("threads") == "2"
