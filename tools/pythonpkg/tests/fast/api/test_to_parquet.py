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

    def test_field_ids_auto(self):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        rel = duckdb.sql('''SELECT {i: 128} AS my_struct''')
        rel.to_parquet(temp_file_name, field_ids='auto')
        parquet_rel = duckdb.read_parquet(temp_file_name)
        assert rel.execute().fetchall() == parquet_rel.execute().fetchall()

    def test_field_ids(self):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        rel = duckdb.sql('''SELECT 1 as i, {j: 128} AS my_struct''')
        rel.to_parquet(temp_file_name, field_ids=dict(i=42, my_struct={'__duckdb_field_id': 43, 'j': 44}))
        parquet_rel = duckdb.read_parquet(temp_file_name)
        assert rel.execute().fetchall() == parquet_rel.execute().fetchall()
        assert (
            [('duckdb_schema', None), ('i', 42), ('my_struct', 43), ('j', 44)]
            == duckdb.sql(
                f'''
            select name,field_id
            from parquet_schema('{temp_file_name}')
        '''
            )
            .execute()
            .fetchall()
        )

    @pytest.mark.parametrize('row_group_size_bytes', [122880 * 1024, '2MB'])
    def test_row_group_size_bytes(self, row_group_size_bytes):
        con = duckdb.connect()
        con.execute("SET preserve_insertion_order=false;")

        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        df = pd.DataFrame({'a': ['string1', 'string2', 'string3']})
        rel = con.from_df(df)
        rel.to_parquet(temp_file_name, row_group_size_bytes=row_group_size_bytes)
        parquet_rel = con.read_parquet(temp_file_name)
        assert rel.execute().fetchall() == parquet_rel.execute().fetchall()

    def test_row_group_size(self):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        df = pd.DataFrame({'a': ['string1', 'string2', 'string3']})
        rel = duckdb.from_df(df)
        rel.to_parquet(temp_file_name, row_group_size=122880)
        parquet_rel = duckdb.read_parquet(temp_file_name)
        assert rel.execute().fetchall() == parquet_rel.execute().fetchall()
