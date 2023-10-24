import duckdb
import tempfile
import os
import pandas._testing as tm
import datetime
import csv
import pytest
from conftest import NumpyPandas, ArrowPandas


class TestToCSV(object):
    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_basic_to_csv(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        df = pandas.DataFrame({'a': [5, 3, 23, 2], 'b': [45, 234, 234, 2]})
        rel = duckdb.from_df(df)

        rel.to_csv(temp_file_name, header=False)

        csv_rel = duckdb.read_csv(temp_file_name)
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_sep(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        df = pandas.DataFrame({'a': [5, 3, 23, 2], 'b': [45, 234, 234, 2]})
        rel = duckdb.from_df(df)

        rel.to_csv(temp_file_name, sep=',', header=False)

        csv_rel = duckdb.read_csv(temp_file_name, sep=',')
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_na_rep(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        df = pandas.DataFrame({'a': [5, None, 23, 2], 'b': [45, 234, 234, 2]})
        rel = duckdb.from_df(df)

        rel.to_csv(temp_file_name, na_rep="test", header=False)

        csv_rel = duckdb.read_csv(temp_file_name, na_values="test")
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_header(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        df = pandas.DataFrame({'a': [5, None, 23, 2], 'b': [45, 234, 234, 2]})
        rel = duckdb.from_df(df)

        rel.to_csv(temp_file_name, header=True)

        csv_rel = duckdb.read_csv(temp_file_name, header=True)
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_quotechar(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        df = pandas.DataFrame({'a': ["\'a,b,c\'", None, "hello", "bye"], 'b': [45, 234, 234, 2]})
        rel = duckdb.from_df(df)

        rel.to_csv(temp_file_name, quotechar='\'', sep=',', header=False)

        csv_rel = duckdb.read_csv(temp_file_name, sep=',', quotechar='\'')
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_escapechar(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        df = pandas.DataFrame(
            {
                "c_bool": [True, False],
                "c_float": [1.0, 3.2],
                "c_int": [42, None],
                "c_string": ["a", "b,c"],
            }
        )
        rel = duckdb.from_df(df)
        rel.to_csv(temp_file_name, header=True, quotechar='"', escapechar='!')
        csv_rel = duckdb.read_csv(temp_file_name, quotechar='"', escapechar='!')
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_date_format(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        df = pandas.DataFrame(tm.getTimeSeriesData())
        dt_index = df.index
        df = pandas.DataFrame({"A": dt_index, "B": dt_index.shift(1)}, index=dt_index)
        rel = duckdb.from_df(df)
        rel.to_csv(temp_file_name, date_format="%Y%m%d", header=False)

        csv_rel = duckdb.read_csv(temp_file_name, date_format="%Y%m%d")

        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_timestamp_format(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        data = [datetime.time(hour=23, minute=1, second=34, microsecond=234345)]
        df = pandas.DataFrame({'0': pandas.Series(data=data, dtype='object')})
        rel = duckdb.from_df(df)
        rel.to_csv(temp_file_name, timestamp_format='%m/%d/%Y', header=False)

        csv_rel = duckdb.read_csv(temp_file_name, timestamp_format='%m/%d/%Y')

        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_quoting_off(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        df = pandas.DataFrame({'a': ['string1', 'string2', 'string3']})
        rel = duckdb.from_df(df)
        rel.to_csv(temp_file_name, quoting=None, header=False)

        csv_rel = duckdb.read_csv(temp_file_name)
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_quoting_on(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        df = pandas.DataFrame({'a': ['string1', 'string2', 'string3']})
        rel = duckdb.from_df(df)
        rel.to_csv(temp_file_name, quoting="force", header=False)

        csv_rel = duckdb.read_csv(temp_file_name)
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_quoting_quote_all(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        df = pandas.DataFrame({'a': ['string1', 'string2', 'string3']})
        rel = duckdb.from_df(df)
        rel.to_csv(temp_file_name, quoting=csv.QUOTE_ALL, header=False)

        csv_rel = duckdb.read_csv(temp_file_name)
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_encoding_incorrect(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        df = pandas.DataFrame({'a': ['string1', 'string2', 'string3']})
        rel = duckdb.from_df(df)
        with pytest.raises(
            duckdb.InvalidInputException, match="Invalid Input Error: The only supported encoding option is 'UTF8"
        ):
            rel.to_csv(temp_file_name, encoding="nope", header=False)

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_encoding_correct(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        df = pandas.DataFrame({'a': ['string1', 'string2', 'string3']})
        rel = duckdb.from_df(df)
        rel.to_csv(temp_file_name, encoding="UTF-8", header=False)
        csv_rel = duckdb.read_csv(temp_file_name)
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_compression_gzip(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        df = pandas.DataFrame({'a': ['string1', 'string2', 'string3']})
        rel = duckdb.from_df(df)
        rel.to_csv(temp_file_name, compression="gzip", header=False)
        csv_rel = duckdb.read_csv(temp_file_name, compression="gzip")
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()
