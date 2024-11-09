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

    @pytest.mark.parametrize('write_columns', [None, True, False])
    def test_partition(self, write_columns):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        df = pd.DataFrame({
                "name": ["rei", "shinji", "asuka", "kaworu"],
                "float": [321.0, 123.0, 23.0, 340.0],
                "category": ['a', 'a', 'b', 'c'],
        })
        rel = duckdb.from_df(df)
        rel.to_parquet(temp_file_name, partition_by=["category"], write_partition_columns=write_columns)
        result = duckdb.sql(f"FROM read_parquet('{temp_file_name}/*/*.parquet', hive_partitioning=TRUE)")
        expected = [
            ("rei", 321.0, "a"),
            ("shinji", 123.0, "a"),
            ("asuka", 23.0, "b"),
            ("kaworu", 340.0, "c")
        ]
        assert result.execute().fetchall() == expected

    @pytest.mark.parametrize('write_columns', [None, True, False])
    def test_overwrite(self, write_columns):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        df = pd.DataFrame(
            {
                "name": ["rei", "shinji", "asuka", "kaworu"],
                "float": [321.0, 123.0, 23.0, 340.0],
                "category": ['a', 'a', 'b', 'c'],
            }
        )
        rel = duckdb.from_df(df)
        rel.to_parquet(temp_file_name, partition_by=["category"], write_partition_columns=write_columns)
        rel.to_parquet(temp_file_name, partition_by=["category"], overwrite=True, write_partition_columns=write_columns)
        result = duckdb.sql(f"FROM read_parquet('{temp_file_name}/*/*.parquet', hive_partitioning=TRUE)")
        expected = [
            ("rei", 321.0, "a"),
            ("shinji", 123.0, "a"),
            ("asuka", 23.0, "b"),
            ("kaworu", 340.0, "c")
        ]

        assert result.execute().fetchall() == expected

    def test_use_tmp_file(self):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        df = pd.DataFrame(
            {
                "name": ["rei", "shinji", "asuka", "kaworu"],
                "float": [321.0, 123.0, 23.0, 340.0],
                "category": ['a', 'a', 'b', 'c'],
            }
        )
        rel = duckdb.from_df(df)
        rel.to_parquet(temp_file_name)
        rel.to_parquet(temp_file_name,  use_tmp_file=True)
        result = duckdb.read_parquet(temp_file_name)
        assert rel.execute().fetchall() == result.execute().fetchall()

    def test_per_thread_output(self):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        num_threads = duckdb.sql("select current_setting('threads')").fetchone()[0]
        print('threads:', num_threads)
        df = pd.DataFrame(
            {
                "name": ["rei", "shinji", "asuka", "kaworu"],
                "float": [321.0, 123.0, 23.0, 340.0],
                "category": ['a', 'a', 'b', 'c'],
            }
        )
        rel = duckdb.from_df(df)
        rel.to_parquet(temp_file_name,per_thread_output=True)
        result = duckdb.read_parquet(f'{temp_file_name}/*.parquet')
        assert rel.execute().fetchall() == result.execute().fetchall()
