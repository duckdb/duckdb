import duckdb
import os
import pandas._testing as tm
import datetime
import csv
import pytest
from conftest import NumpyPandas, ArrowPandas, getTimeSeriesData


@pytest.fixture(scope="function")
def temp_file_name(request, tmp_path_factory):
    return str(tmp_path_factory.mktemp(request.function.__name__, numbered=True) / 'file.csv')


class TestToCSV(object):
    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_basic_to_csv(self, pandas, temp_file_name):
        df = pandas.DataFrame({'a': [5, 3, 23, 2], 'b': [45, 234, 234, 2]})
        rel = duckdb.from_df(df)

        rel.to_csv(temp_file_name)

        csv_rel = duckdb.read_csv(temp_file_name)
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_sep(self, pandas, temp_file_name):
        df = pandas.DataFrame({'a': [5, 3, 23, 2], 'b': [45, 234, 234, 2]})
        rel = duckdb.from_df(df)

        rel.to_csv(temp_file_name, sep=',')

        csv_rel = duckdb.read_csv(temp_file_name, sep=',')
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_na_rep(self, pandas, temp_file_name):
        df = pandas.DataFrame({'a': [5, None, 23, 2], 'b': [45, 234, 234, 2]})
        rel = duckdb.from_df(df)

        rel.to_csv(temp_file_name, na_rep="test")

        csv_rel = duckdb.read_csv(temp_file_name, na_values="test")
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_header(self, pandas, temp_file_name):
        df = pandas.DataFrame({'a': [5, None, 23, 2], 'b': [45, 234, 234, 2]})
        rel = duckdb.from_df(df)

        rel.to_csv(temp_file_name)

        csv_rel = duckdb.read_csv(temp_file_name)
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_quotechar(self, pandas, temp_file_name):
        df = pandas.DataFrame({'a': ["\'a,b,c\'", None, "hello", "bye"], 'b': [45, 234, 234, 2]})
        rel = duckdb.from_df(df)

        rel.to_csv(temp_file_name, quotechar='\'', sep=',')

        csv_rel = duckdb.read_csv(temp_file_name, sep=',', quotechar='\'')
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_escapechar(self, pandas, temp_file_name):
        df = pandas.DataFrame(
            {
                "c_bool": [True, False],
                "c_float": [1.0, 3.2],
                "c_int": [42, None],
                "c_string": ["a", "b,c"],
            }
        )
        rel = duckdb.from_df(df)
        rel.to_csv(temp_file_name, quotechar='"', escapechar='!')
        csv_rel = duckdb.read_csv(temp_file_name, quotechar='"', escapechar='!')
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_date_format(self, pandas, temp_file_name):
        df = pandas.DataFrame(getTimeSeriesData())
        dt_index = df.index
        df = pandas.DataFrame({"A": dt_index, "B": dt_index.shift(1)}, index=dt_index)
        rel = duckdb.from_df(df)
        rel.to_csv(temp_file_name, date_format="%Y%m%d")

        csv_rel = duckdb.read_csv(temp_file_name, date_format="%Y%m%d")

        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_timestamp_format(self, pandas, temp_file_name):
        data = [datetime.time(hour=23, minute=1, second=34, microsecond=234345)]
        df = pandas.DataFrame({'0': pandas.Series(data=data, dtype='object')})
        rel = duckdb.from_df(df)
        rel.to_csv(temp_file_name, timestamp_format='%m/%d/%Y')

        csv_rel = duckdb.read_csv(temp_file_name, timestamp_format='%m/%d/%Y')

        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_quoting_off(self, pandas, temp_file_name):
        df = pandas.DataFrame({'a': ['string1', 'string2', 'string3']})
        rel = duckdb.from_df(df)
        rel.to_csv(temp_file_name, quoting=None)

        csv_rel = duckdb.read_csv(temp_file_name)
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_quoting_on(self, pandas, temp_file_name):
        df = pandas.DataFrame({'a': ['string1', 'string2', 'string3']})
        rel = duckdb.from_df(df)
        rel.to_csv(temp_file_name, quoting="force")

        csv_rel = duckdb.read_csv(temp_file_name)
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_quoting_quote_all(self, pandas, temp_file_name):
        df = pandas.DataFrame({'a': ['string1', 'string2', 'string3']})
        rel = duckdb.from_df(df)
        rel.to_csv(temp_file_name, quoting=csv.QUOTE_ALL)

        csv_rel = duckdb.read_csv(temp_file_name)
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_encoding_incorrect(self, pandas, temp_file_name):
        df = pandas.DataFrame({'a': ['string1', 'string2', 'string3']})
        rel = duckdb.from_df(df)
        with pytest.raises(
            duckdb.InvalidInputException, match="Invalid Input Error: The only supported encoding option is 'UTF8"
        ):
            rel.to_csv(temp_file_name, encoding="nope")

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_encoding_correct(self, pandas, temp_file_name):
        df = pandas.DataFrame({'a': ['string1', 'string2', 'string3']})
        rel = duckdb.from_df(df)
        rel.to_csv(temp_file_name, encoding="UTF-8")
        csv_rel = duckdb.read_csv(temp_file_name)
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_compression_gzip(self, pandas, temp_file_name):
        df = pandas.DataFrame({'a': ['string1', 'string2', 'string3']})
        rel = duckdb.from_df(df)
        rel.to_csv(temp_file_name, compression="gzip")
        csv_rel = duckdb.read_csv(temp_file_name, compression="gzip")
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_partition(self, pandas, temp_file_name):
        df = pandas.DataFrame(
            {
                "c_category": ['a', 'a', 'b', 'b'],
                "c_bool": [True, False, True, True],
                "c_float": [1.0, 3.2, 3.0, 4.0],
                "c_int": [42, None, 123, 321],
                "c_string": ["a", "b,c", "e", "f"],
            }
        )
        rel = duckdb.from_df(df)
        rel.to_csv(temp_file_name, header=True, partition_by=["c_category"])
        csv_rel = duckdb.sql(
            f'''FROM read_csv_auto('{temp_file_name}/*/*.csv', hive_partitioning=TRUE, header=TRUE);'''
        )
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_overwrite(self, pandas, temp_file_name):
        df = pandas.DataFrame(
            {
                "c_category_1": ['a', 'a', 'b', 'b'],
                "c_category_2": ['c', 'c', 'd', 'd'],
                "c_bool": [True, False, True, True],
                "c_float": [1.0, 3.2, 3.0, 4.0],
                "c_int": [42, None, 123, 321],
                "c_string": ["a", "b,c", "e", "f"],
            }
        )
        rel = duckdb.from_df(df)
        rel.to_csv(temp_file_name, header=True, partition_by=["c_category_1"])  # csv to be overwritten
        rel.to_csv(temp_file_name, header=True, partition_by=["c_category_1"], overwrite=True)
        csv_rel = duckdb.sql(
            f'''FROM read_csv_auto('{temp_file_name}/*/*.csv', hive_partitioning=TRUE, header=TRUE);'''
        )
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_overwrite_not_enabled(self, pandas, temp_file_name):
        df = pandas.DataFrame(
            {
                "c_category_1": ['a', 'a', 'b', 'b'],
                "c_category_2": ['c', 'c', 'd', 'd'],
                "c_bool": [True, False, True, True],
                "c_float": [1.0, 3.2, 3.0, 4.0],
                "c_int": [42, None, 123, 321],
                "c_string": ["a", "b,c", "e", "f"],
            }
        )
        rel = duckdb.from_df(df)
        rel.to_csv(temp_file_name, header=True, partition_by=["c_category_1"])
        with pytest.raises(duckdb.IOException, match="Enable OVERWRITE_OR_IGNORE option to force writing"):
            rel.to_csv(temp_file_name, header=True, partition_by=["c_category_1"])

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_per_thread_output(self, pandas, temp_file_name, duckdb_cursor):
        num_threads = duckdb_cursor.sql("select current_setting('threads')").fetchone()[0]
        print('num_threads:', num_threads)
        df = pandas.DataFrame(
            {
                "c_category": ['a', 'a', 'b', 'b'],
                "c_bool": [True, False, True, True],
                "c_float": [1.0, 3.2, 3.0, 4.0],
                "c_int": [42, None, 123, 321],
                "c_string": ["a", "b,c", "e", "f"],
            }
        )
        rel = duckdb_cursor.from_df(df)
        rel.to_csv(temp_file_name, header=True, per_thread_output=True)
        created_files = duckdb_cursor.sql(f"select * from glob('{temp_file_name}/*.csv')").fetchall()
        print(created_files)
        for i, item in enumerate(created_files):
            print(f'File {i}')
            print(open(item[0]).read())

        csv_rel = duckdb_cursor.read_csv(f'{temp_file_name}/*.csv', header=True)
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_use_tmp_file(self, pandas, temp_file_name):
        df = pandas.DataFrame(
            {
                "c_category_1": ['a', 'a', 'b', 'b'],
                "c_category_2": ['c', 'c', 'd', 'd'],
                "c_bool": [True, False, True, True],
                "c_float": [1.0, 3.2, 3.0, 4.0],
                "c_int": [42, None, 123, 321],
                "c_string": ["a", "b,c", "e", "f"],
            }
        )
        rel = duckdb.from_df(df)
        rel.to_csv(temp_file_name, header=True)  # csv to be overwritten
        rel.to_csv(temp_file_name, header=True, use_tmp_file=True)
        csv_rel = duckdb.read_csv(temp_file_name, header=True)
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()
