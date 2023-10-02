import pytest

_ = pytest.importorskip("duckdb.experimental.spark")


class TestSparkSession(object):
    def test_read_csv_basic(self, spark):
        pass
        # spark.read.csv()
