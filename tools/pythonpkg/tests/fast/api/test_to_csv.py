import duckdb
import tempfile
import os
import pandas as pd
import tempfile

class TestToCSV(object):
    def test_basic_to_csv(self):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        df = pd.DataFrame({'a': [5,3,23,2], 'b': [45,234,234,2]})
        rel = duckdb.from_df(df)

        rel.to_csv(temp_file_name)

        csv_rel = duckdb.read_csv(temp_file_name)
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    def test_to_csv_sep(self):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        df = pd.DataFrame({'a': [5,3,23,2], 'b': [45,234,234,2]})
        rel = duckdb.from_df(df)

        rel.to_csv(temp_file_name, sep=',')

        csv_rel = duckdb.read_csv(temp_file_name, sep=',')
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    def test_to_csv_na_rep(self):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        df = pd.DataFrame({'a': [5,None,23,2], 'b': [45,234,234,2]})
        rel = duckdb.from_df(df)

        rel.to_csv(temp_file_name, na_rep="test")

        csv_rel = duckdb.read_csv(temp_file_name, na_values="test")
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    def test_to_csv_header(self):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        df = pd.DataFrame({'a': [5,None,23,2], 'b': [45,234,234,2]})
        rel = duckdb.from_df(df)

        rel.to_csv(temp_file_name, header=True)

        csv_rel = duckdb.read_csv(temp_file_name, header=True)
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    def test_to_csv_quotechar(self):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        df = pd.DataFrame({'a': ["\'a,b,c\'",None,"hello","bye"], 'b': [45,234,234,2]})
        rel = duckdb.from_df(df)

        rel.to_csv(temp_file_name, quotechar='\'', sep=',')

        csv_rel = duckdb.read_csv(temp_file_name, sep=',', quotechar='\'')
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    def test_to_csv_escapechar(self):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        df = pd.DataFrame(
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
