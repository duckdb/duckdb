import pytest

_ = pytest.importorskip("pyduckdb.spark")
from pyduckdb.spark.sql import SparkSession


class TestSparkSession(object):
    def test_spark_session_default(self):
        session = SparkSession.builder.getOrCreate()

    def test_spark_session(self):
        session = SparkSession.builder.master("local[1]").appName('SparkByExamples.com').getOrCreate()

    def test_new_session(self, spark: SparkSession):
        session = spark.newSession()
        print(session)

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

    def test_table(self, spark):
        spark.sql('create table tbl(a varchar(10))')
        df = spark.table('tbl')

    def test_udf(self, spark):
        with pytest.raises(NotImplementedError):
            udf_registration = spark.udf
