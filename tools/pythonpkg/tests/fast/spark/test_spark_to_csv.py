import pytest
import tempfile

import os

_ = pytest.importorskip("duckdb.experimental.spark")

from duckdb.experimental.spark.sql import SparkSession as session
from duckdb import connect, InvalidInputException, read_csv
from conftest import NumpyPandas, ArrowPandas
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


class TestSparkToCSV(object):
    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_basic_to_csv(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))

        pandas_df = pandas.DataFrame({'a': [5, 3, 23, 2], 'b': [45, 234, 234, 2]})

        spark_session = session.builder.getOrCreate()
        df = spark_session.createDataFrame(pandas_df)

        df.write.csv(temp_file_name, header=False)

        csv_rel = spark_session.read.csv(temp_file_name, header=False)

        assert df.collect() == csv_rel.collect()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_sep(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        pandas_df = pandas.DataFrame({'a': [5, 3, 23, 2], 'b': [45, 234, 234, 2]})

        spark_session = session.builder.getOrCreate()
        df = spark_session.createDataFrame(pandas_df)

        df.write.csv(temp_file_name, sep=',', header=False)

        csv_rel = spark_session.read.csv(temp_file_name, header=False, sep=',')
        assert df.collect() == csv_rel.collect()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_na_rep(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        pandas_df = pandas.DataFrame({'a': [5, None, 23, 2], 'b': [45, 234, 234, 2]})

        spark_session = session.builder.getOrCreate()
        df = spark_session.createDataFrame(pandas_df)

        df.write.csv(temp_file_name, nullValue="test", header=False)

        csv_rel = spark_session.read.csv(temp_file_name, nullValue="test")
        assert df.collect() == csv_rel.collect()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_header(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        pandas_df = pandas.DataFrame({'a': [5, None, 23, 2], 'b': [45, 234, 234, 2]})

        spark_session = session.builder.getOrCreate()
        df = spark_session.createDataFrame(pandas_df)        

        df.write.csv(temp_file_name, header=True)

        csv_rel = spark_session.read.csv(temp_file_name, header=True)
        assert df.collect() == csv_rel.collect()
    
    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_quotechar(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        
        pandas_df = pandas.DataFrame({'a': ["\'a,b,c\'", None, "hello", "bye"], 'b': [45, 234, 234, 2]})
        
        spark_session = session.builder.getOrCreate()
        df = spark_session.createDataFrame(pandas_df)        
        
        df.write.csv(temp_file_name, quote='\'', sep=',', header=False)

        csv_rel = spark_session.read.csv(temp_file_name, sep=',', quote='\'')
        assert df.collect() == csv_rel.collect()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_escapechar(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        pandas_df = pandas.DataFrame(            {
                "c_bool": [True, False],
                "c_float": [1.0, 3.2],
                "c_int": [42, None],
                "c_string": ["a", "b,c"],
            })
        
        spark_session = session.builder.getOrCreate()
        df = spark_session.createDataFrame(pandas_df)   

        df.write.csv(temp_file_name, header=True, quote='"', escape='!')
        # ask a question about this - is header = False the default behaviour in Spark
        csv_rel = spark_session.read.csv(temp_file_name, quote='"', escape='!', header=True)
        assert df.collect() == csv_rel.collect()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_date_format(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        pandas_df = pandas.DataFrame(tm.getTimeSeriesData())
        dt_index = pandas_df.index
        pandas_df = pandas.DataFrame({"A": dt_index, "B": dt_index.shift(1)}, index=dt_index)
        
        spark_session = session.builder.getOrCreate()
        df = spark_session.createDataFrame(pandas_df)

        df.write.csv(temp_file_name, dateFormat="%Y%m%d", header=False)

        csv_rel = spark_session.read.csv(temp_file_name, dateFormat="%Y%m%d")

        assert df.collect() == csv_rel.collect()


    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_timestamp_format(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        data = [datetime.time(hour=23, minute=1, second=34, microsecond=234345)]
        pandas_df = pandas.DataFrame({'0': pandas.Series(data=data, dtype='object')})
        
        spark_session = session.builder.getOrCreate()
        df = spark_session.createDataFrame(pandas_df)
        
        df.write.csv(temp_file_name, timestampFormat='%m/%d/%Y', header=False)

        csv_rel = spark_session.read.csv(temp_file_name, timestampFormat='%m/%d/%Y')

        assert df.collect() == csv_rel.collect()


    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_quoting_off(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        pandas_df = pandas.DataFrame({'a': ['string1', 'string2', 'string3']})
        spark_session = session.builder.getOrCreate()
        
        df = spark_session.createDataFrame(pandas_df)
        df.write.csv(temp_file_name, quoteAll=None, header=False)

        csv_rel = spark_session.read.csv(temp_file_name)
        assert df.collect() == csv_rel.collect()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_quoting_on(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        pandas_df = pandas.DataFrame({'a': ['string1', 'string2', 'string3']})
        spark_session = session.builder.getOrCreate()
        df = spark_session.createDataFrame(pandas_df)
        df.write.csv(temp_file_name, quoteAll="force", header=False)

        csv_rel = spark_session.read.csv(temp_file_name)
        assert df.collect() == csv_rel.collect()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_quoting_quote_all(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        pandas_df = pandas.DataFrame({'a': ['string1', 'string2', 'string3']})
        spark_session = session.builder.getOrCreate()
        df = spark_session.createDataFrame(pandas_df)
        df.write.csv(temp_file_name, quoteAll=csv.QUOTE_ALL, header=False)

        csv_rel = spark_session.read.csv(temp_file_name)
        assert df.collect() == csv_rel.collect()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_encoding_incorrect(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        pandas_df = pandas.DataFrame({'a': ['string1', 'string2', 'string3']})
        spark_session = session.builder.getOrCreate()
        df = spark_session.createDataFrame(pandas_df)
        with pytest.raises(
            InvalidInputException, match="Invalid Input Error: The only supported encoding option is 'UTF8"
        ):
            df.write.csv(temp_file_name, encoding="nope", header=False)

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_encoding_correct(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        pandas_df = pandas.DataFrame({'a': ['string1', 'string2', 'string3']})
        spark_session = session.builder.getOrCreate()
        df = spark_session.createDataFrame(pandas_df)
        df.write.csv(temp_file_name, encoding="UTF-8", header=False)
        csv_rel = spark_session.read.csv(temp_file_name)
        assert df.collect() == csv_rel.collect()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_compression_gzip(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        pandas_df = pandas.DataFrame({'a': ['string1', 'string2', 'string3']})
        spark_session = session.builder.getOrCreate()
        df = spark_session.createDataFrame(pandas_df)
        df.write.csv(temp_file_name, compression="gzip", header=False)
        
        # slightly convoluted - pyspark .read.csv does not take a compression argument
        csv_rel = spark_session.createDataFrame(read_csv(temp_file_name, compression="gzip").df())
        assert df.collect() == csv_rel.collect()