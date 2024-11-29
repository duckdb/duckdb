import pytest

_ = pytest.importorskip("duckdb.experimental.spark")

from spark_namespace.sql.types import Row
from spark_namespace import USE_ACTUAL_SPARK
import textwrap


class TestSparkReadCSV(object):
    def test_read_csv(self, spark, tmp_path):
        file_path = tmp_path / 'basic.csv'
        with open(file_path, 'w+') as f:
            f.write(
                textwrap.dedent(
                    """
				1,2
				3,4
				5,6
			"""
                )
            )
        file_path = file_path.as_posix()
        df = spark.read.csv(file_path)
        res = df.collect()

        expected_res = sorted([Row(column0=1, column1=2), Row(column0=3, column1=4), Row(column0=5, column1=6)])
        if USE_ACTUAL_SPARK:
            # Convert all values to strings as this is how Spark reads them by default
            expected_res = [Row(column0=str(row.column0), column1=str(row.column1)) for row in expected_res]
        assert sorted(res) == expected_res
