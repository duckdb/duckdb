import duckdb
import tempfile
import os
import pandas._testing as tm
import datetime
import csv
import pytest
from conftest import NumpyPandas, ArrowPandas, getTimeSeriesData


class TestToCSV(object):
    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_basic_to_csv(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        df = pandas.DataFrame({'a': [5, 3, 23, 2], 'b': [45, 234, 234, 2]})
        rel = duckdb.from_df(df)

        rel.to_csv(temp_file_name)

        csv_rel = duckdb.read_csv(temp_file_name)
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_sep(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        df = pandas.DataFrame({'a': [5, 3, 23, 2], 'b': [45, 234, 234, 2]})
        rel = duckdb.from_df(df)

        rel.to_csv(temp_file_name, sep=',')

        csv_rel = duckdb.read_csv(temp_file_name, sep=',')
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_na_rep(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        df = pandas.DataFrame({'a': [5, None, 23, 2], 'b': [45, 234, 234, 2]})
        rel = duckdb.from_df(df)

        rel.to_csv(temp_file_name, na_rep="test")

        csv_rel = duckdb.read_csv(temp_file_name, na_values="test")
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_header(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        df = pandas.DataFrame({'a': [5, None, 23, 2], 'b': [45, 234, 234, 2]})
        rel = duckdb.from_df(df)

        rel.to_csv(temp_file_name)

        csv_rel = duckdb.read_csv(temp_file_name)
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_quotechar(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        df = pandas.DataFrame({'a': ["\'a,b,c\'", None, "hello", "bye"], 'b': [45, 234, 234, 2]})
        rel = duckdb.from_df(df)

        rel.to_csv(temp_file_name, quotechar='\'', sep=',')

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
        rel.to_csv(temp_file_name, quotechar='"', escapechar='!')
        csv_rel = duckdb.read_csv(temp_file_name, quotechar='"', escapechar='!')
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_date_format(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        df = pandas.DataFrame(getTimeSeriesData())
        dt_index = df.index
        df = pandas.DataFrame({"A": dt_index, "B": dt_index.shift(1)}, index=dt_index)
        rel = duckdb.from_df(df)
        rel.to_csv(temp_file_name, date_format="%Y%m%d")

        csv_rel = duckdb.read_csv(temp_file_name, date_format="%Y%m%d")

        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_timestamp_format(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        data = [datetime.time(hour=23, minute=1, second=34, microsecond=234345)]
        df = pandas.DataFrame({'0': pandas.Series(data=data, dtype='object')})
        rel = duckdb.from_df(df)
        rel.to_csv(temp_file_name, timestamp_format='%m/%d/%Y')

        csv_rel = duckdb.read_csv(temp_file_name, timestamp_format='%m/%d/%Y')

        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_quoting_off(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        df = pandas.DataFrame({'a': ['string1', 'string2', 'string3']})
        rel = duckdb.from_df(df)
        rel.to_csv(temp_file_name, quoting=None)

        csv_rel = duckdb.read_csv(temp_file_name)
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_quoting_on(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        df = pandas.DataFrame({'a': ['string1', 'string2', 'string3']})
        rel = duckdb.from_df(df)
        rel.to_csv(temp_file_name, quoting="force")

        csv_rel = duckdb.read_csv(temp_file_name)
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_quoting_quote_all(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        df = pandas.DataFrame({'a': ['string1', 'string2', 'string3']})
        rel = duckdb.from_df(df)
        rel.to_csv(temp_file_name, quoting=csv.QUOTE_ALL)

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
            rel.to_csv(temp_file_name, encoding="nope")

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_encoding_correct(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        df = pandas.DataFrame({'a': ['string1', 'string2', 'string3']})
        rel = duckdb.from_df(df)
        rel.to_csv(temp_file_name, encoding="UTF-8")
        csv_rel = duckdb.read_csv(temp_file_name)
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_compression_gzip(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        df = pandas.DataFrame({'a': ['string1', 'string2', 'string3']})
        rel = duckdb.from_df(df)
        rel.to_csv(temp_file_name, compression="gzip")
        csv_rel = duckdb.read_csv(temp_file_name, compression="gzip")
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_partition(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
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
        expected = [
            (True, 1.0, 42.0, 'a', 'a'),
            (False, 3.2, None, 'b,c', 'a'),
            (True, 3.0, 123.0, 'e', 'b'),
            (True, 4.0, 321.0, 'f', 'b'),
        ]

        assert csv_rel.execute().fetchall() == expected

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_partition_with_columns_written(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
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
        res = duckdb.sql("FROM rel order by all")
        rel.to_csv(temp_file_name, header=True, partition_by=["c_category"], write_partition_columns=True)
        csv_rel = duckdb.sql(
            f'''FROM read_csv_auto('{temp_file_name}/*/*.csv', hive_partitioning=TRUE, header=TRUE) order by all;'''
        )
        assert res.execute().fetchall() == csv_rel.execute().fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_overwrite(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
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
        # When partition columns are read from directory names, column order become different from original
        expected = [
            ('c', True, 1.0, 42.0, 'a', 'a'),
            ('c', False, 3.2, None, 'b,c', 'a'),
            ('d', True, 3.0, 123.0, 'e', 'b'),
            ('d', True, 4.0, 321.0, 'f', 'b'),
        ]
        assert csv_rel.execute().fetchall() == expected

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_overwrite_with_columns_written(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
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
        rel.to_csv(
            temp_file_name, header=True, partition_by=["c_category_1"], write_partition_columns=True
        )  # csv to be overwritten
        rel.to_csv(
            temp_file_name, header=True, partition_by=["c_category_1"], overwrite=True, write_partition_columns=True
        )
        csv_rel = duckdb.sql(
            f'''FROM read_csv_auto('{temp_file_name}/*/*.csv', hive_partitioning=TRUE, header=TRUE) order by all;'''
        )
        res = duckdb.sql("FROM rel order by all")
        assert res.execute().fetchall() == csv_rel.execute().fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_overwrite_not_enabled(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
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
        with pytest.raises(duckdb.IOException, match="OVERWRITE"):
            rel.to_csv(temp_file_name, header=True, partition_by=["c_category_1"])

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_per_thread_output(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        num_threads = duckdb.sql("select current_setting('threads')").fetchone()[0]
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
        rel = duckdb.from_df(df)
        rel.to_csv(temp_file_name, header=True, per_thread_output=True)
        csv_rel = duckdb.read_csv(f'{temp_file_name}/*.csv', header=True)
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_to_csv_use_tmp_file(self, pandas):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
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
