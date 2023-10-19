import pytest
import duckdb

VECTOR_SIZE = duckdb.__standard_vector_size__


class TestType(object):
    def test_fetch_df_chunk(self):
        size = 3000
        con = duckdb.connect()
        con.execute(f"CREATE table t as select range a from range({size});")
        query = con.execute("SELECT a FROM t")

        # Fetch the first chunk
        cur_chunk = query.fetch_df_chunk()
        assert cur_chunk['a'][0] == 0
        assert len(cur_chunk) == VECTOR_SIZE

        # Fetch the second chunk, can't be entirely filled
        cur_chunk = query.fetch_df_chunk()
        assert cur_chunk['a'][0] == VECTOR_SIZE
        expected = size - VECTOR_SIZE
        assert len(cur_chunk) == expected

    @pytest.mark.parametrize('size', [3000, 10000, 100000, VECTOR_SIZE - 1, VECTOR_SIZE + 1, VECTOR_SIZE])
    def test_monahan(self, size):
        con = duckdb.connect()
        con.execute(f"CREATE table t as select range a from range({size});")
        query = con.execute("SELECT a FROM t")

        processed = 0
        expected_chunks = size // VECTOR_SIZE
        if size % VECTOR_SIZE != 0:
            expected_chunks += 1

        for _ in range(expected_chunks):
            expected_size = size - processed
            if expected_size > VECTOR_SIZE:
                expected_size = VECTOR_SIZE
            cur_chunk = query.fetch_df_chunk()
            assert len(cur_chunk) == expected_size
            processed += expected_size

        cur_chunk = query.fetch_df_chunk()
        assert len(cur_chunk) == 0

    def test_fetch_df_chunk_parameter(self):
        size = 10000
        con = duckdb.connect()
        con.execute(f"CREATE table t as select range a from range({size});")
        query = con.execute("SELECT a FROM t")

        # Return 2 vectors
        cur_chunk = query.fetch_df_chunk(2)
        assert cur_chunk['a'][0] == 0
        assert len(cur_chunk) == VECTOR_SIZE * 2

        # Return Default 1 vector
        cur_chunk = query.fetch_df_chunk()
        assert cur_chunk['a'][0] == VECTOR_SIZE * 2
        assert len(cur_chunk) == VECTOR_SIZE

        # Return 0 vectors
        cur_chunk = query.fetch_df_chunk(0)
        assert len(cur_chunk) == 0

        fetched = VECTOR_SIZE * 3
        expected = size - fetched

        # Return more vectors than we have remaining
        cur_chunk = query.fetch_df_chunk(3)
        assert cur_chunk['a'][0] == fetched
        assert len(cur_chunk) == expected

        # These shouldn't throw errors (Just emmit empty chunks)
        cur_chunk = query.fetch_df_chunk(100)
        assert len(cur_chunk) == 0

        cur_chunk = query.fetch_df_chunk(0)
        assert len(cur_chunk) == 0

        cur_chunk = query.fetch_df_chunk()
        assert len(cur_chunk) == 0

    def test_fetch_df_chunk_negative_parameter(self):
        con = duckdb.connect()
        con.execute("CREATE table t as select range a from range(100);")
        query = con.execute("SELECT a FROM t")

        # Return -1 vector should not work
        with pytest.raises(TypeError, match='incompatible function arguments'):
            cur_chunk = query.fetch_df_chunk(-1)
