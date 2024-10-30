import pytest

_ = pytest.importorskip("duckdb.experimental.spark")
pa = pytest.importorskip("pyarrow")
from spark_namespace import USE_ACTUAL_SPARK


class TestArrowTable:
    def test_spark_to_arrow_table(self, spark):
        if USE_ACTUAL_SPARK:
            return
        data = [
            ("firstRowFirstColumn",),
            ("2ndRowFirstColumn",),
        ]
        df = spark.createDataFrame(data, ["firstColumn"])
        arrow_table = df.toArrow()
        assert arrow_table.num_columns == 1
        assert arrow_table.num_rows == 2
        assert arrow_table.column_names == ["firstColumn"]
