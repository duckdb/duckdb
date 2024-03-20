import pytest
import tempfile

import os

_ = pytest.importorskip("duckdb.experimental.spark")

from duckdb.experimental.spark.sql import SparkSession as session
from duckdb import connect, InvalidInputException, read_csv
from conftest import NumpyPandas, ArrowPandas, getTimeSeriesData
import pandas._testing as tm
import datetime
import csv


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


@pytest.fixture(params=[NumpyPandas(), ArrowPandas()])
def pandas_df_ints(request, spark):
    pandas = request.param
    dataframe = pandas.DataFrame({'a': [5, 3, 23, 2], 'b': [45, 234, 234, 2]})
    yield dataframe


@pytest.fixture(params=[NumpyPandas(), ArrowPandas()])
def pandas_df_strings(request, spark):
    pandas = request.param
    dataframe = pandas.DataFrame({'a': ['string1', 'string2', 'string3']})
    yield dataframe


class TestSparkToCSV(object):
    def test_basic_to_csv(self, pandas_df_ints, spark, tmp_path):
        temp_file_name = os.path.join(tmp_path, "temp_file.csv")

        df = spark.createDataFrame(pandas_df_ints)

        df.write.csv(temp_file_name)

        csv_rel = spark.read.csv(temp_file_name)

        assert df.collect() == csv_rel.collect()

    def test_to_csv_sep(self, pandas_df_ints, spark, tmp_path):
        temp_file_name = os.path.join(tmp_path, "temp_file.csv")

        df = spark.createDataFrame(pandas_df_ints)

        df.write.csv(temp_file_name, sep=',')

        csv_rel = spark.read.csv(temp_file_name, sep=',')
        assert df.collect() == csv_rel.collect()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_na_rep(self, pandas, spark, tmp_path):
        temp_file_name = os.path.join(tmp_path, "temp_file.csv")
        pandas_df = pandas.DataFrame({'a': [5, None, 23, 2], 'b': [45, 234, 234, 2]})

        df = spark.createDataFrame(pandas_df)

        df.write.csv(temp_file_name, nullValue="test")

        csv_rel = spark.read.csv(temp_file_name, nullValue="test")
        assert df.collect() == csv_rel.collect()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_header(self, pandas, spark, tmp_path):
        temp_file_name = os.path.join(tmp_path, "temp_file.csv")
        pandas_df = pandas.DataFrame({'a': [5, None, 23, 2], 'b': [45, 234, 234, 2]})

        df = spark.createDataFrame(pandas_df)

        df.write.csv(temp_file_name)

        csv_rel = spark.read.csv(temp_file_name)
        assert df.collect() == csv_rel.collect()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_quotechar(self, pandas, spark, tmp_path):
        temp_file_name = os.path.join(tmp_path, "temp_file.csv")

        pandas_df = pandas.DataFrame({'a': ["\'a,b,c\'", None, "hello", "bye"], 'b': [45, 234, 234, 2]})

        df = spark.createDataFrame(pandas_df)

        df.write.csv(temp_file_name, quote='\'', sep=',')

        csv_rel = spark.read.csv(temp_file_name, sep=',', quote='\'')
        assert df.collect() == csv_rel.collect()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_escapechar(self, pandas, spark, tmp_path):
        temp_file_name = os.path.join(tmp_path, "temp_file.csv")
        pandas_df = pandas.DataFrame(
            {
                "c_bool": [True, False],
                "c_float": [1.0, 3.2],
                "c_int": [42, None],
                "c_string": ["a", "b,c"],
            }
        )

        df = spark.createDataFrame(pandas_df)

        df.write.csv(temp_file_name, quote='"', escape='!')
        csv_rel = spark.read.csv(temp_file_name, quote='"', escape='!')
        assert df.collect() == csv_rel.collect()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_date_format(self, pandas, spark, tmp_path):
        temp_file_name = os.path.join(tmp_path, "temp_file.csv")
        pandas_df = pandas.DataFrame(getTimeSeriesData())
        dt_index = pandas_df.index
        pandas_df = pandas.DataFrame({"A": dt_index, "B": dt_index.shift(1)}, index=dt_index)

        df = spark.createDataFrame(pandas_df)

        df.write.csv(temp_file_name, dateFormat="%Y%m%d")

        csv_rel = spark.read.csv(temp_file_name, dateFormat="%Y%m%d")

        assert df.collect() == csv_rel.collect()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_timestamp_format(self, pandas, spark, tmp_path):
        temp_file_name = os.path.join(tmp_path, "temp_file.csv")
        data = [datetime.time(hour=23, minute=1, second=34, microsecond=234345)]
        pandas_df = pandas.DataFrame({'0': pandas.Series(data=data, dtype='object')})

        df = spark.createDataFrame(pandas_df)

        df.write.csv(temp_file_name, timestampFormat='%m/%d/%Y')

        csv_rel = spark.read.csv(temp_file_name, timestampFormat='%m/%d/%Y')

        assert df.collect() == csv_rel.collect()

    def test_to_csv_quoting_off(self, pandas_df_strings, spark, tmp_path):
        temp_file_name = os.path.join(tmp_path, "temp_file.csv")
        df = spark.createDataFrame(pandas_df_strings)
        df.write.csv(temp_file_name, quoteAll=None)

        csv_rel = spark.read.csv(temp_file_name)
        assert df.collect() == csv_rel.collect()

    def test_to_csv_quoting_on(self, pandas_df_strings, spark, tmp_path):
        temp_file_name = os.path.join(tmp_path, "temp_file.csv")
        df = spark.createDataFrame(pandas_df_strings)
        df.write.csv(temp_file_name, quoteAll="force")

        csv_rel = spark.read.csv(temp_file_name)
        assert df.collect() == csv_rel.collect()

    def test_to_csv_quoting_quote_all(self, pandas_df_strings, spark, tmp_path):
        temp_file_name = os.path.join(tmp_path, "temp_file.csv")
        df = spark.createDataFrame(pandas_df_strings)
        df.write.csv(temp_file_name, quoteAll=csv.QUOTE_ALL)

        csv_rel = spark.read.csv(temp_file_name)
        assert df.collect() == csv_rel.collect()

    def test_to_csv_encoding_incorrect(self, pandas_df_strings, spark, tmp_path):
        temp_file_name = os.path.join(tmp_path, "temp_file.csv")
        df = spark.createDataFrame(pandas_df_strings)
        with pytest.raises(
            InvalidInputException, match="Invalid Input Error: The only supported encoding option is 'UTF8"
        ):
            df.write.csv(temp_file_name, encoding="nope")

    def test_to_csv_encoding_correct(self, pandas_df_strings, spark, tmp_path):
        temp_file_name = os.path.join(tmp_path, "temp_file.csv")
        df = spark.createDataFrame(pandas_df_strings)
        df.write.csv(temp_file_name, encoding="UTF-8")
        csv_rel = spark.read.csv(temp_file_name)
        assert df.collect() == csv_rel.collect()

    def test_compression_gzip(self, pandas_df_strings, spark, tmp_path):
        temp_file_name = os.path.join(tmp_path, "temp_file.csv")
        df = spark.createDataFrame(pandas_df_strings)
        df.write.csv(temp_file_name, compression="gzip")

        # slightly convoluted - pyspark .read.csv does not take a compression argument
        csv_rel = spark.createDataFrame(read_csv(temp_file_name, compression="gzip", header=False).df())
        print(df.collect())
        print(csv_rel.collect())
        assert df.collect() == csv_rel.collect()
