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

        csv_rel = duckdb.from_csv_auto(temp_file_name)
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    def test_to_csv_sep(self):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        df = pd.DataFrame({'a': [5,3,23,2], 'b': [45,234,234,2]})
        rel = duckdb.from_df(df)

        rel.to_csv(temp_file_name, sep=',')

        csv_rel = duckdb.from_csv_auto(temp_file_name)
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    def test_to_csv_sep(self):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        df = pd.DataFrame({'a': [5,None,23,2], 'b': [45,234,234,2]})
        rel = duckdb.from_df(df)

        rel.to_csv(temp_file_name, na_rep="test")

        csv_rel = duckdb.from_csv_auto(temp_file_name)
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()
