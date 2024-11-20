import pytest
import tempfile

import os

_ = pytest.importorskip("duckdb.experimental.spark")


@pytest.fixture
def df(spark):
    simpleData = (
        ("Java", 4000, 5),
        ("Python", 4600, 10),
        ("Scala", 4100, 15),
        ("Scala", 4500, 15),
        ("PHP", 3000, 20),
    )
    columns = ["CourseName", "fee", "discount"]
    dataframe = spark.createDataFrame(data=simpleData, schema=columns)
    yield dataframe


class TestSparkToParquet(object):
    def test_basic_to_parquet(self, df, spark, tmp_path):
        temp_file_name = os.path.join(tmp_path, "temp_file.parquet")

        df.write.parquet(temp_file_name)

        csv_rel = spark.read.parquet(temp_file_name)

        assert sorted(df.collect()) == sorted(csv_rel.collect())

    def test_compressed_to_parquet(self, df, spark, tmp_path):
        temp_file_name = os.path.join(tmp_path, "temp_file.parquet")

        df.write.parquet(temp_file_name, compression="ZSTD")

        csv_rel = spark.read.parquet(temp_file_name)

        assert sorted(df.collect()) == sorted(csv_rel.collect())
