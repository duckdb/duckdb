import pytest

_ = pytest.importorskip("pyduckdb.spark")


class TestSparkRuntimeConfig(object):
    def test_spark_runtime_config(self, spark):
        # This fetches the internal runtime config from the session
        spark.conf

    def test_spark_runtime_config_set(self, spark):
        # Set Config
        with pytest.raises(NotImplementedError):
            spark.conf.set("spark.executor.memory", "5g")

    @pytest.mark.skip(reason="RuntimeConfig is not implemented yet")
    def test_spark_runtime_config_get(self, spark):
        # Get a Spark Config
        with pytest.raises(KeyError):
            partitions = spark.conf.get("spark.sql.shuffle.partitions")
