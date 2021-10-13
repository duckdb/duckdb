import duckdb
import pytest
try:
    import pyarrow as pa
    can_run = True
except:
    can_run = False


class TestArrowFetchChunk(object):
    def test_fetch_arrow_chunk(self, duckdb_cursor):
        if not can_run:
            return

        duckdb_cursor.execute("CREATE table t as select range a from range(3000);")
        query = duckdb_cursor.execute("SELECT a FROM t")
        cur_chunk = query.fetch_arrow_chunk(return_table=True)
        assert(len(cur_chunk) == 1024)
        cur_chunk = query.fetch_arrow_chunk(return_table=True)
        assert(len(cur_chunk) == 1024)
        cur_chunk = query.fetch_arrow_chunk(return_table=True)
        assert(len(cur_chunk) == 952)

    def test_arrow_empty(self,duckdb_cursor):
        if not can_run:
            return

        duckdb_cursor.execute("CREATE table t as select range a from range(3000);")
        query = duckdb_cursor.execute("SELECT a FROM t")
        cur_chunk = query.fetch_arrow_chunk(return_table=True)
        cur_chunk = query.fetch_arrow_chunk(return_table=True)
        cur_chunk = query.fetch_arrow_chunk(return_table=True)
        #Should be empty by now
        try:
            cur_chunk = query.fetch_arrow_chunk(return_table=True)
            print(cur_chunk)
        except Exception as err:
            print(err)
            
        #Should be empty by now
        try:
            cur_chunk = query.fetch_arrow_chunk(return_table=True)
            print(cur_chunk)
        except Exception as err:
            print(err)
 
    def test_fetch_arrow_chunk_parameter(self, duckdb_cursor):
        if not can_run:
            return

        duckdb_cursor.execute("CREATE table t as select range a from range(10000);")
        query = duckdb_cursor.execute("SELECT a FROM t")
        
        # Return 2 vectors
        cur_chunk = query.fetch_arrow_chunk(2,True)
        assert(len(cur_chunk) == 2048)
        
        # Return Default 1 vector
        cur_chunk = query.fetch_arrow_chunk(return_table=True)
        assert(len(cur_chunk) == 1024)
        
        # Return 3 vectors
        cur_chunk = query.fetch_arrow_chunk(3,True)
        assert(len(cur_chunk) == 3072)
        
        # Return 0 vectors
        cur_chunk = query.fetch_arrow_chunk(0,True)
        assert(len(cur_chunk) == 0)


        # Return 1 vector
        cur_chunk = query.fetch_arrow_chunk(1,True)
        assert(len(cur_chunk) == 1024)

         # Return more vectors than we have remaining
        cur_chunk = query.fetch_arrow_chunk(100,True)
        assert(len(cur_chunk) == 2832)

        # This should return 0
        cur_chunk = query.fetch_arrow_chunk(100,True)
        assert(len(cur_chunk) == 0)


    def test_fetch_arrow_chunk_negative_parameter(self, duckdb_cursor):
        if not can_run:
            return
            
        duckdb_cursor.execute("CREATE table t as select range a from range(100);")
        query = duckdb_cursor.execute("SELECT a FROM t")
        
        # Return -1 vector should not work
        with pytest.raises(Exception):
            cur_chunk = query.fetch_arrow_chunk(-1,True)

    def test_fetch_record_batches(self,duckdb_cursor):
        if not can_run:
            return
        duckdb_cursor.execute("CREATE table t as select range a from range(3000);")
        query = duckdb_cursor.execute("SELECT a FROM t")
        
        record_batch_list = query.fetch_arrow_chunk()
        assert(len(record_batch_list) == 1)
        assert(len(record_batch_list[0]) == 1024)
        
        record_batch_list = query.fetch_arrow_chunk(2)
        assert(len(record_batch_list) == 2)
        assert(len(record_batch_list[0]) == 1024)
        assert(len(record_batch_list[1]) == 952)