import pytest

_ = pytest.importorskip("duckdb.experimental.spark")


class TestSparkUDF(object):
    def test_udf_register(self, spark):

        def to_upper_fn(s: str) -> str:
            return s.upper()

        spark.udf.register("to_upper_fn", to_upper_fn)
        assert spark.sql("select to_upper_fn('quack') as vl").collect()[0].vl == "QUACK"
