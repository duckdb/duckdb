import pytest

_ = pytest.importorskip("duckdb.experimental.spark")

from duckdb.experimental.spark.sql.types import Row
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
        assert res == [Row(column0=1, column1=2), Row(column0=3, column1=4), Row(column0=5, column1=6)]
