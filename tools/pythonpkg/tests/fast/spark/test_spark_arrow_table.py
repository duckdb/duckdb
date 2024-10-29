import pytest

_ = pytest.importorskip("duckdb.experimental.spark")
pa = pytest.importorskip("pyarrow")


class TestArrowTable:
    def test_spark_to_arrow_table(self, spark):
        data = [
            ("firstRowFirstColumn",),
            ("2ndRowFirstColumn",),
        ]
        df = spark.createDataFrame(data, ["firstColumn"])
        arrow_table = df.toArrow()
        assert arrow_table.num_columns == 1
        assert arrow_table.num_rows == 2
        assert arrow_table.column_names == ["firstColumn"]
