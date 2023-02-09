import duckdb
import tempfile
import os
import pandas as pd
import pandas._testing as tm
import datetime
import csv
import pytest

class TestToCSV(object):
    def test_basic_to_csv(self, tmp_path):
        # Convert to string because we don't accept anything other than 'str' for now
        file_name = str(tmp_path / 'file.csv')
        con = duckdb.connect()
        df = pd.DataFrame({'a': [5,3,23,2], 'b': [45,234,234,2]})
        rel = con.from_df(df)

        rel.to_csv(file_name)

        csv_rel = con.read_csv(file_name)
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    def test_to_csv_sep(self, tmp_path):
        # Convert to string because we don't accept anything other than 'str' for now
        file_name = str(tmp_path / 'file.csv')
        con = duckdb.connect()
        df = pd.DataFrame({'a': [5,3,23,2], 'b': [45,234,234,2]})
        rel = con.from_df(df)

        rel.to_csv(file_name, sep=',')

        csv_rel = con.read_csv(file_name, sep=',')
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    def test_to_csv_na_rep(self, tmp_path):
        # Convert to string because we don't accept anything other than 'str' for now
        file_name = str(tmp_path / 'file.csv')
        con = duckdb.connect()
        df = pd.DataFrame({'a': [5,None,23,2], 'b': [45,234,234,2]})
        rel = con.from_df(df)

        rel.to_csv(file_name, na_rep="test")

        csv_rel = con.read_csv(file_name, na_values="test")
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    def test_to_csv_header(self, tmp_path):
        # Convert to string because we don't accept anything other than 'str' for now
        file_name = str(tmp_path / 'file.csv')
        con = duckdb.connect()
        df = pd.DataFrame({'a': [5,None,23,2], 'b': [45,234,234,2]})
        rel = con.from_df(df)

        rel.to_csv(file_name, header=True)

        csv_rel = con.read_csv(file_name, header=True)
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    def test_to_csv_quotechar(self, tmp_path):
        # Convert to string because we don't accept anything other than 'str' for now
        file_name = str(tmp_path / 'file.csv')
        con = duckdb.connect()
        df = pd.DataFrame({'a': ["\'a,b,c\'",None,"hello","bye"], 'b': [45,234,234,2]})
        rel = con.from_df(df)

        rel.to_csv(file_name, quotechar='\'', sep=',')

        csv_rel = con.read_csv(file_name, sep=',', quotechar='\'')
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    def test_to_csv_escapechar(self, tmp_path):
        # Convert to string because we don't accept anything other than 'str' for now
        file_name = str(tmp_path / 'file.csv')
        con = duckdb.connect()
        df = pd.DataFrame(
            {
                "c_bool": [True, False],
                "c_float": [1.0, 3.2],
                "c_int": [42, None],
                "c_string": ["a", "b,c"],
            }
        )
        rel = con.from_df(df)
        rel.to_csv(file_name, header=True, quotechar='"', escapechar='!')
        csv_rel = con.read_csv(file_name, quotechar='"', escapechar='!')
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    def test_to_csv_date_format(self, tmp_path):
        # Convert to string because we don't accept anything other than 'str' for now
        file_name = str(tmp_path / 'file.csv')
        con = duckdb.connect()
        df = pd.DataFrame(tm.getTimeSeriesData())
        dt_index = df.index
        df = pd.DataFrame(
            {"A": dt_index, "B": dt_index.shift(1)}, index=dt_index
        )
        rel = con.from_df(df)
        rel.to_csv(file_name, date_format="%Y%m%d")

        csv_rel = con.read_csv(file_name, date_format="%Y%m%d")

        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    def test_to_csv_timestamp_format(self, tmp_path):
        # Convert to string because we don't accept anything other than 'str' for now
        file_name = str(tmp_path / 'file.csv')
        con = duckdb.connect()
        data = [datetime.time(hour=23, minute=1, second=34, microsecond=234345)]
        df = pd.DataFrame(
            {'0': pd.Series(data=data, dtype='object')}
        )
        rel = con.from_df(df)
        rel.to_csv(file_name, timestamp_format='%m/%d/%Y')

        csv_rel = con.read_csv(file_name, timestamp_format='%m/%d/%Y')

        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    def test_to_csv_quoting_off(self, tmp_path):
        # Convert to string because we don't accept anything other than 'str' for now
        file_name = str(tmp_path / 'file.csv')
        con = duckdb.connect()
        df = pd.DataFrame(
            {'a': ['string1', 'string2', 'string3']}
        )
        rel = con.from_df(df)
        rel.to_csv(file_name, quoting=None)

        csv_rel = con.read_csv(file_name)
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    def test_to_csv_quoting_on(self, tmp_path):
        # Convert to string because we don't accept anything other than 'str' for now
        file_name = str(tmp_path / 'file.csv')
        con = duckdb.connect()
        df = pd.DataFrame(
            {'a': ['string1', 'string2', 'string3']}
        )
        rel = con.from_df(df)
        rel.to_csv(file_name, quoting="force")

        csv_rel = con.read_csv(file_name)
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    def test_to_csv_quoting_quote_all(self, tmp_path):
        # Convert to string because we don't accept anything other than 'str' for now
        file_name = str(tmp_path / 'file.csv')
        con = duckdb.connect()
        df = pd.DataFrame(
            {'a': ['string1', 'string2', 'string3']}
        )
        rel = con.from_df(df)
        rel.to_csv(file_name, quoting=csv.QUOTE_ALL)

        csv_rel = con.read_csv(file_name)
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    def test_to_csv_encoding_incorrect(self, tmp_path):
        # Convert to string because we don't accept anything other than 'str' for now
        file_name = str(tmp_path / 'file.csv')
        con = duckdb.connect()
        df = pd.DataFrame(
            {'a': ['string1', 'string2', 'string3']}
        )
        rel = con.from_df(df)
        with pytest.raises(duckdb.InvalidInputException, match="Invalid Input Error: The only supported encoding option is 'UTF8"):
            rel.to_csv(file_name, encoding="nope")

    def test_to_csv_encoding_correct(self, tmp_path):
        # Convert to string because we don't accept anything other than 'str' for now
        file_name = str(tmp_path / 'file.csv')
        con = duckdb.connect()
        df = pd.DataFrame(
            {'a': ['string1', 'string2', 'string3']}
        )
        rel = con.from_df(df)
        rel.to_csv(file_name, encoding="UTF-8")
        csv_rel = con.read_csv(file_name)
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    def test_compression_gzip(self, tmp_path):
        # Convert to string because we don't accept anything other than 'str' for now
        file_name = str(tmp_path / 'file.csv')
        con = duckdb.connect()
        df = pd.DataFrame(
            {'a': ['string1', 'string2', 'string3']}
        )
        rel = con.from_df(df)
        rel.to_csv(file_name, compression="gzip")
        csv_rel = con.read_csv(file_name, compression="gzip")
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()
