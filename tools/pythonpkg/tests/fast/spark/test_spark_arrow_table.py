import pytest

_ = pytest.importorskip("duckdb.experimental.spark")
pa = pytest.importorskip("pyarrow")
from spark_namespace import USE_ACTUAL_SPARK

from spark_namespace import USE_ACTUAL_SPARK
from spark_namespace.sql.dataframe import DataFrame


class TestArrowTable:
    @pytest.mark.skipif(
        USE_ACTUAL_SPARK and not hasattr(DataFrame, "toArrow"),
        reason="toArrow is only introduced in PySpark 4.0.0",
    )
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
