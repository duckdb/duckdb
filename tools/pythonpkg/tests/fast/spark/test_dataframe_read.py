import pytest

_ = pytest.importorskip("duckdb.spark")


class TestSparkSession(object):
    @pytest.mark.skip("Reading CSV to DataFrame is not implemented yet")
    def test_read_csv_basic(self, spark):
        pass
