import pytest
import duckdb


class TestType(object):
    def test_fetch_df_chunk(self, duckdb_cursor):
        duckdb_cursor.execute("CREATE table t as select range a from range(3000);")
        query = duckdb_cursor.execute("SELECT a FROM t")
        cur_chunk = query.fetch_df_chunk()
        assert cur_chunk['a'][0] == 0
        assert len(cur_chunk) == 2048
        cur_chunk = query.fetch_df_chunk()
        assert cur_chunk['a'][0] == 2048
        assert len(cur_chunk) == 952
        duckdb_cursor.execute("DROP TABLE t")

    def test_monahan(self, duckdb_cursor):
        duckdb_cursor.execute("CREATE table t as select range a from range(3000);")
        query = duckdb_cursor.execute("SELECT a FROM t")
        cur_chunk = query.fetch_df_chunk()
        print(cur_chunk)
        cur_chunk = query.fetch_df_chunk()
        print(cur_chunk)
        cur_chunk = query.fetch_df_chunk()
        print(cur_chunk)
        # Should be empty by now
        try:
            cur_chunk = query.fetch_df_chunk()
            print(cur_chunk)
        except Exception as err:
            print(err)

        # Should be empty by now
        try:
            cur_chunk = query.fetch_df_chunk()
            print(cur_chunk)
        except Exception as err:
            print(err)
        duckdb_cursor.execute("DROP TABLE t")

    def test_fetch_df_chunk_parameter(self, duckdb_cursor):
        duckdb_cursor.execute("CREATE table t as select range a from range(10000);")
        query = duckdb_cursor.execute("SELECT a FROM t")

        # Return 2 vectors
        cur_chunk = query.fetch_df_chunk(2)
        assert cur_chunk['a'][0] == 0
        assert len(cur_chunk) == 4096

        # Return Default 1 vector
        cur_chunk = query.fetch_df_chunk()
        assert cur_chunk['a'][0] == 4096
        assert len(cur_chunk) == 2048

        # Return 0 vectors
        cur_chunk = query.fetch_df_chunk(0)
        assert len(cur_chunk) == 0

        # Return more vectors than we have remaining
        cur_chunk = query.fetch_df_chunk(3)
        assert cur_chunk['a'][0] == 6144
        assert len(cur_chunk) == 3856

        # These shouldn't throw errors (Just emmit empty chunks)
        cur_chunk = query.fetch_df_chunk(100)
        assert len(cur_chunk) == 0

        cur_chunk = query.fetch_df_chunk(0)
        assert len(cur_chunk) == 0

        cur_chunk = query.fetch_df_chunk()
        assert len(cur_chunk) == 0
        duckdb_cursor.execute("DROP TABLE t")

    def test_fetch_df_chunk_negative_parameter(self, duckdb_cursor):
        duckdb_cursor.execute("CREATE table t as select range a from range(100);")
        query = duckdb_cursor.execute("SELECT a FROM t")

        # Return -1 vector should not work
        with pytest.raises(TypeError, match='incompatible function arguments'):
            cur_chunk = query.fetch_df_chunk(-1)
        duckdb_cursor.execute("DROP TABLE t")
