import duckdb
import pytest
try:
    import pyarrow as pa
    can_run = True
except:
    can_run = False


class TestArrowFetchRecordBatch(object):
    def test_record_batch_next_batch(self, duckdb_cursor):
        if not can_run:
            return
        duckdb_cursor = duckdb.connect()
        duckdb_cursor.execute("CREATE table t as select range a from range(3000);")
        query = duckdb_cursor.execute("SELECT a FROM t")
        record_batch_reader = query.fetch_record_batch()
        chunk = record_batch_reader.read_next_batch()
        assert(len(chunk) == 1024)
        chunk = record_batch_reader.read_next_batch()
        assert(len(chunk) == 1024)
        chunk = record_batch_reader.read_next_batch()
        assert(len(chunk) == 952)
        # StopIteration Exception
        with pytest.raises(Exception):
            chunk = record_batch_reader.read_next_batch()

    def test_record_batch_read_all(self, duckdb_cursor):
        if not can_run:
            return
        duckdb_cursor = duckdb.connect()
        duckdb_cursor.execute("CREATE table t as select range a from range(3000);")
        query = duckdb_cursor.execute("SELECT a FROM t")
        record_batch_reader = query.fetch_record_batch()
        chunk = record_batch_reader.read_all()
        assert(len(chunk) == 3000)
       

