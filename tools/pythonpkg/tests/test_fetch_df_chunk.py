class TestType(object):

    def test_fetch_df_chunk(self, duckdb_cursor):
        duckdb_cursor.execute("CREATE table t as select range a from range(3000);")
        query = duckdb_cursor.execute("SELECT a FROM t")
        cur_chunk = query.fetch_df_chunk()
        assert(cur_chunk['a'][0] == 0)
        assert(len(cur_chunk) == 1024)
        cur_chunk = query.fetch_df_chunk()
        assert(cur_chunk['a'][0] == 1024)
        assert(len(cur_chunk) == 1024)
        cur_chunk = query.fetch_df_chunk()
        assert(cur_chunk['a'][0] == 2048)
        assert(len(cur_chunk) == 952)
 