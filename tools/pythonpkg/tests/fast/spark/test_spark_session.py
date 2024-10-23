import pytest
from duckdb.experimental.spark.exception import (
    ContributionsAcceptedError,
)
from spark_namespace.sql.types import Row
from spark_namespace import USE_ACTUAL_SPARK

_ = pytest.importorskip("duckdb.experimental.spark")
from spark_namespace.sql import SparkSession


class TestSparkSession(object):
    def test_spark_session_default(self):
        session = SparkSession.builder.getOrCreate()

    def test_spark_session(self):
        session = SparkSession.builder.master("local[1]").appName('SparkByExamples.com').getOrCreate()

    def test_new_session(self, spark: SparkSession):
        session = spark.newSession()

    @pytest.mark.skip(reason='not tested yet')
    def test_retrieve_same_session(self):
        spark = SparkSession.builder.master('test').appName('test2').getOrCreate()
        spark2 = SparkSession.builder.getOrCreate()
        # Same connection should be returned
        assert spark == spark2

    def test_config(self):
        # Usage of config()
        spark = (
            SparkSession.builder.master("local[1]")
            .appName("SparkByExamples.com")
            .config("spark.some.config.option", "config-value")
            .getOrCreate()
        )

    @pytest.mark.skip(reason="enableHiveSupport is not implemented yet")
    def test_hive_support(self):
        # Enabling Hive to use in Spark
        spark = (
            SparkSession.builder.master("local[1]")
            .appName("SparkByExamples.com")
            .config("spark.sql.warehouse.dir", "<path>/spark-warehouse")
            .enableHiveSupport()
            .getOrCreate()
        )

    @pytest.mark.skipif(USE_ACTUAL_SPARK, reason="Different version numbers")
    def test_version(self, spark):
        version = spark.version
        assert version == '1.0.0'

    def test_get_active_session(self, spark):
        active_session = spark.getActiveSession()

    def test_read(self, spark):
        reader = spark.read

    def test_write(self, spark):
        df = spark.sql('select 42')
        writer = df.write

    def test_read_stream(self, spark):
        reader = spark.readStream

    def test_spark_context(self, spark):
        context = spark.sparkContext

    def test_sql(self, spark):
        df = spark.sql('select 42')

    def test_stop_context(self, spark):
        context = spark.sparkContext
        spark.stop()

    @pytest.mark.skipif(
        USE_ACTUAL_SPARK, reason="Can't create table with the local PySpark setup in the CI/CD pipeline"
    )
    def test_table(self, spark):
        spark.sql('create table tbl(a varchar(10))')
        df = spark.table('tbl')

    def test_range(self, spark):
        res_1 = spark.range(3).collect()
        res_2 = spark.range(3, 10, 2).collect()
        res_3 = spark.range(3, 6).collect()

        assert res_1 == [Row(id=0), Row(id=1), Row(id=2)]
        assert res_2 == [Row(id=3), Row(id=5), Row(id=7), Row(id=9)]
        assert res_3 == [Row(id=3), Row(id=4), Row(id=5)]

        if not USE_ACTUAL_SPARK:
            with pytest.raises(ContributionsAcceptedError):
                # partition size is not supported
                spark.range(0, 10, 2, 2)

    def test_udf(self, spark):
        udf_registration = spark.udf
