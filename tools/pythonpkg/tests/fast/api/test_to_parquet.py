import duckdb
import tempfile
import os
import pandas as pd
import tempfile
import pandas._testing as tm
import datetime
import csv
import pytest


class TestToParquet(object):
    def test_basic_to_parquet(self):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        df = pd.DataFrame({'a': [5, 3, 23, 2], 'b': [45, 234, 234, 2]})
        rel = duckdb.from_df(df)

        rel.to_parquet(temp_file_name)

        csv_rel = duckdb.read_parquet(temp_file_name)
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()

    def test_compression_gzip(self):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        df = pd.DataFrame({'a': ['string1', 'string2', 'string3']})
        rel = duckdb.from_df(df)
        rel.to_parquet(temp_file_name, compression="gzip")
        csv_rel = duckdb.read_parquet(temp_file_name, compression="gzip")
        assert rel.execute().fetchall() == csv_rel.execute().fetchall()
