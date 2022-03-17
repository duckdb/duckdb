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
        record_batch_reader = query.fetch_record_batch(1024)
        assert record_batch_reader.schema.names == ['a']
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
        record_batch_reader = query.fetch_record_batch(1024)
        chunk = record_batch_reader.read_all()
        assert(len(chunk) == 3000)

    def test_record_batch_read_default(self, duckdb_cursor):
        if not can_run:
            return
        duckdb_cursor = duckdb.connect()
        duckdb_cursor.execute("CREATE table t as select range a from range(3000);")
        query = duckdb_cursor.execute("SELECT a FROM t")
        record_batch_reader = query.fetch_record_batch()
        chunk = record_batch_reader.read_next_batch()
        assert(len(chunk) == 3000)

    def test_record_batch_next_batch_multiple_vectors_per_chunk(self, duckdb_cursor):
        if not can_run:
            return
        duckdb_cursor = duckdb.connect()
        duckdb_cursor.execute("CREATE table t as select range a from range(5000);")
        query = duckdb_cursor.execute("SELECT a FROM t")
        record_batch_reader = query.fetch_record_batch(2048)
        chunk = record_batch_reader.read_next_batch()
        assert(len(chunk) == 2048)
        chunk = record_batch_reader.read_next_batch()
        assert(len(chunk) == 2048)
        chunk = record_batch_reader.read_next_batch()
        assert(len(chunk) == 904)
        # StopIteration Exception
        with pytest.raises(Exception):
            chunk = record_batch_reader.read_next_batch()

        query = duckdb_cursor.execute("SELECT a FROM t")
        record_batch_reader = query.fetch_record_batch(1)
        chunk = record_batch_reader.read_next_batch()
        assert(len(chunk) == 1024)

        query = duckdb_cursor.execute("SELECT a FROM t")
        record_batch_reader = query.fetch_record_batch(2000)
        chunk = record_batch_reader.read_next_batch()
        assert(len(chunk) == 2048)
        

    def test_record_batch_next_batch_multiple_vectors_per_chunk_error(self, duckdb_cursor):
        if not can_run:
            return
        duckdb_cursor = duckdb.connect()
        duckdb_cursor.execute("CREATE table t as select range a from range(5000);")
        query = duckdb_cursor.execute("SELECT a FROM t")
        with pytest.raises(Exception):
            record_batch_reader = query.fetch_record_batch(0)
        with pytest.raises(Exception):
            record_batch_reader = query.fetch_record_batch(-1)

    def test_record_batch_reader_from_relation(self, duckdb_cursor):
        if not can_run:
            return
        duckdb_cursor = duckdb.connect()
        duckdb_cursor.execute("CREATE table t as select range a from range(3000);")
        relation = duckdb_cursor.table('t')
        record_batch_reader = relation.record_batch()
        chunk = record_batch_reader.read_next_batch()
        assert(len(chunk) == 3000)

    def test_record_coverage(self, duckdb_cursor):
        if not can_run:
            return
        duckdb_cursor = duckdb.connect()
        duckdb_cursor.execute("CREATE table t as select range a from range(2048);")
        query = duckdb_cursor.execute("SELECT a FROM t")
        record_batch_reader = query.fetch_record_batch(1024)

        chunk = record_batch_reader.read_all()
        assert(len(chunk) == 2048)

