import duckdb
import pytest

pa = pytest.importorskip('pyarrow')


class TestArrowFetchRecordBatch(object):
    # Test with basic numeric conversion (integers, floats, and others fall this code-path)
    def test_record_batch_next_batch_numeric(self, duckdb_cursor):
        duckdb_cursor = duckdb.connect()
        duckdb_cursor_check = duckdb.connect()
        duckdb_cursor.execute("CREATE table t as select range a from range(3000);")
        query = duckdb_cursor.execute("SELECT a FROM t")
        record_batch_reader = query.fetch_record_batch(1024)
        assert record_batch_reader.schema.names == ['a']
        chunk = record_batch_reader.read_next_batch()
        assert len(chunk) == 1024
        chunk = record_batch_reader.read_next_batch()
        assert len(chunk) == 1024
        chunk = record_batch_reader.read_next_batch()
        assert len(chunk) == 952
        with pytest.raises(StopIteration):
            chunk = record_batch_reader.read_next_batch()
        # Check if we are producing the correct thing
        query = duckdb_cursor.execute("SELECT a FROM t")
        record_batch_reader = query.fetch_record_batch(1024)

        res = duckdb_cursor_check.execute("select * from record_batch_reader").fetchall()
        correct = duckdb_cursor.execute("select * from t").fetchall()
        assert res == correct

    # Test With Bool
    def test_record_batch_next_batch_bool(self, duckdb_cursor):
        duckdb_cursor = duckdb.connect()
        duckdb_cursor_check = duckdb.connect()
        duckdb_cursor.execute(
            "CREATE table t as SELECT CASE WHEN i % 2 = 0 THEN true ELSE false END AS a from range(3000) as tbl(i);"
        )
        query = duckdb_cursor.execute("SELECT a FROM t")
        record_batch_reader = query.fetch_record_batch(1024)
        assert record_batch_reader.schema.names == ['a']
        chunk = record_batch_reader.read_next_batch()
        assert len(chunk) == 1024
        chunk = record_batch_reader.read_next_batch()
        assert len(chunk) == 1024
        chunk = record_batch_reader.read_next_batch()
        assert len(chunk) == 952
        with pytest.raises(StopIteration):
            chunk = record_batch_reader.read_next_batch()

        # Check if we are producing the correct thing
        query = duckdb_cursor.execute("SELECT a FROM t")
        record_batch_reader = query.fetch_record_batch(1024)

        res = duckdb_cursor_check.execute("select * from record_batch_reader").fetchall()
        correct = duckdb_cursor.execute("select * from t").fetchall()
        assert res == correct

    # Test with Varchar
    def test_record_batch_next_batch_varchar(self, duckdb_cursor):
        duckdb_cursor = duckdb.connect()
        duckdb_cursor_check = duckdb.connect()
        duckdb_cursor.execute("CREATE table t as select range::varchar a from range(3000);")
        query = duckdb_cursor.execute("SELECT a FROM t")
        record_batch_reader = query.fetch_record_batch(1024)
        assert record_batch_reader.schema.names == ['a']
        chunk = record_batch_reader.read_next_batch()
        assert len(chunk) == 1024
        chunk = record_batch_reader.read_next_batch()
        assert len(chunk) == 1024
        chunk = record_batch_reader.read_next_batch()
        assert len(chunk) == 952
        with pytest.raises(StopIteration):
            chunk = record_batch_reader.read_next_batch()

        # Check if we are producing the correct thing
        query = duckdb_cursor.execute("SELECT a FROM t")
        record_batch_reader = query.fetch_record_batch(1024)

        res = duckdb_cursor_check.execute("select * from record_batch_reader").fetchall()
        correct = duckdb_cursor.execute("select * from t").fetchall()
        assert res == correct

    # Test with Struct
    def test_record_batch_next_batch_struct(self, duckdb_cursor):
        duckdb_cursor = duckdb.connect()
        duckdb_cursor_check = duckdb.connect()
        duckdb_cursor.execute(
            "CREATE table t as select {'x': i, 'y': i::varchar, 'z': i+1} as a from range(3000)  as tbl(i);"
        )
        query = duckdb_cursor.execute("SELECT a FROM t")
        record_batch_reader = query.fetch_record_batch(1024)
        assert record_batch_reader.schema.names == ['a']
        chunk = record_batch_reader.read_next_batch()
        assert len(chunk) == 1024
        chunk = record_batch_reader.read_next_batch()
        assert len(chunk) == 1024
        chunk = record_batch_reader.read_next_batch()
        assert len(chunk) == 952
        with pytest.raises(StopIteration):
            chunk = record_batch_reader.read_next_batch()

        # Check if we are producing the correct thing
        query = duckdb_cursor.execute("SELECT a FROM t")
        record_batch_reader = query.fetch_record_batch(1024)

        res = duckdb_cursor_check.execute("select * from record_batch_reader").fetchall()
        correct = duckdb_cursor.execute("select * from t").fetchall()
        assert res == correct

    # Test with List
    def test_record_batch_next_batch_list(self, duckdb_cursor):
        duckdb_cursor = duckdb.connect()
        duckdb_cursor_check = duckdb.connect()
        duckdb_cursor.execute("CREATE table t as select [i,i+1] as a from range(3000)  as tbl(i);")
        query = duckdb_cursor.execute("SELECT a FROM t")
        record_batch_reader = query.fetch_record_batch(1024)
        assert record_batch_reader.schema.names == ['a']
        chunk = record_batch_reader.read_next_batch()
        assert len(chunk) == 1024
        chunk = record_batch_reader.read_next_batch()
        assert len(chunk) == 1024
        chunk = record_batch_reader.read_next_batch()
        assert len(chunk) == 952
        with pytest.raises(StopIteration):
            chunk = record_batch_reader.read_next_batch()

        # Check if we are producing the correct thing
        query = duckdb_cursor.execute("SELECT a FROM t")
        record_batch_reader = query.fetch_record_batch(1024)

        res = duckdb_cursor_check.execute("select * from record_batch_reader").fetchall()
        correct = duckdb_cursor.execute("select * from t").fetchall()

        assert res == correct

    # Test with Map
    def test_record_batch_next_batch_map(self, duckdb_cursor):
        duckdb_cursor = duckdb.connect()
        duckdb_cursor_check = duckdb.connect()
        duckdb_cursor.execute("CREATE table t as select map([i], [i+1]) as a from range(3000)  as tbl(i);")
        query = duckdb_cursor.execute("SELECT a FROM t")
        record_batch_reader = query.fetch_record_batch(1024)
        assert record_batch_reader.schema.names == ['a']
        chunk = record_batch_reader.read_next_batch()
        assert len(chunk) == 1024
        chunk = record_batch_reader.read_next_batch()
        assert len(chunk) == 1024
        chunk = record_batch_reader.read_next_batch()
        assert len(chunk) == 952
        with pytest.raises(StopIteration):
            chunk = record_batch_reader.read_next_batch()

        # Check if we are producing the correct thing
        query = duckdb_cursor.execute("SELECT a FROM t")
        record_batch_reader = query.fetch_record_batch(1024)

        res = duckdb_cursor_check.execute("select * from record_batch_reader").fetchall()
        correct = duckdb_cursor.execute("select * from t").fetchall()

        assert res == correct

    # Test with Null Values
    def test_record_batch_next_batch_with_null(self, duckdb_cursor):
        duckdb_cursor = duckdb.connect()
        duckdb_cursor_check = duckdb.connect()
        duckdb_cursor.execute(
            "CREATE table t as SELECT CASE WHEN i % 2 = 0 THEN i ELSE NULL END AS a from range(3000) as tbl(i);"
        )
        query = duckdb_cursor.execute("SELECT a FROM t")
        record_batch_reader = query.fetch_record_batch(1024)
        assert record_batch_reader.schema.names == ['a']
        chunk = record_batch_reader.read_next_batch()
        assert len(chunk) == 1024
        chunk = record_batch_reader.read_next_batch()
        assert len(chunk) == 1024
        chunk = record_batch_reader.read_next_batch()
        assert len(chunk) == 952
        with pytest.raises(StopIteration):
            chunk = record_batch_reader.read_next_batch()

        # Check if we are producing the correct thing
        query = duckdb_cursor.execute("SELECT a FROM t")
        record_batch_reader = query.fetch_record_batch(1024)

        res = duckdb_cursor_check.execute("select * from record_batch_reader").fetchall()
        correct = duckdb_cursor.execute("select * from t").fetchall()

        assert res == correct

    def test_record_batch_read_default(self, duckdb_cursor):
        duckdb_cursor = duckdb.connect()
        duckdb_cursor.execute("CREATE table t as select range a from range(3000);")
        query = duckdb_cursor.execute("SELECT a FROM t")
        record_batch_reader = query.fetch_record_batch()
        chunk = record_batch_reader.read_next_batch()
        assert len(chunk) == 3000

    def test_record_batch_next_batch_multiple_vectors_per_chunk(self, duckdb_cursor):
        duckdb_cursor = duckdb.connect()
        duckdb_cursor.execute("CREATE table t as select range a from range(5000);")
        query = duckdb_cursor.execute("SELECT a FROM t")
        record_batch_reader = query.fetch_record_batch(2048)
        chunk = record_batch_reader.read_next_batch()
        assert len(chunk) == 2048
        chunk = record_batch_reader.read_next_batch()
        assert len(chunk) == 2048
        chunk = record_batch_reader.read_next_batch()
        assert len(chunk) == 904
        with pytest.raises(StopIteration):
            chunk = record_batch_reader.read_next_batch()

        query = duckdb_cursor.execute("SELECT a FROM t")
        record_batch_reader = query.fetch_record_batch(1)
        chunk = record_batch_reader.read_next_batch()
        assert len(chunk) == 1

        query = duckdb_cursor.execute("SELECT a FROM t")
        record_batch_reader = query.fetch_record_batch(2000)
        chunk = record_batch_reader.read_next_batch()
        assert len(chunk) == 2000

    def test_record_batch_next_batch_multiple_vectors_per_chunk_error(self, duckdb_cursor):
        duckdb_cursor = duckdb.connect()
        duckdb_cursor.execute("CREATE table t as select range a from range(5000);")
        query = duckdb_cursor.execute("SELECT a FROM t")
        with pytest.raises(RuntimeError, match='Approximate Batch Size of Record Batch MUST be higher than 0'):
            record_batch_reader = query.fetch_record_batch(0)
        with pytest.raises(TypeError, match='incompatible function arguments'):
            record_batch_reader = query.fetch_record_batch(-1)

    def test_record_batch_reader_from_relation(self, duckdb_cursor):
        duckdb_cursor = duckdb.connect()
        duckdb_cursor.execute("CREATE table t as select range a from range(3000);")
        relation = duckdb_cursor.table('t')
        record_batch_reader = relation.record_batch()
        chunk = record_batch_reader.read_next_batch()
        assert len(chunk) == 3000

    def test_record_coverage(self, duckdb_cursor):
        duckdb_cursor = duckdb.connect()
        duckdb_cursor.execute("CREATE table t as select range a from range(2048);")
        query = duckdb_cursor.execute("SELECT a FROM t")
        record_batch_reader = query.fetch_record_batch(1024)

        chunk = record_batch_reader.read_all()
        assert len(chunk) == 2048

    def test_record_batch_query_error(self):
        duckdb_cursor = duckdb.connect()
        duckdb_cursor.execute("CREATE table t as select 'foo' as a;")
        with pytest.raises(duckdb.ConversionException, match='Conversion Error'):
            # 'execute' materializes the result, causing the error directly
            query = duckdb_cursor.execute("SELECT cast(a as double) FROM t")
            record_batch_reader = query.fetch_record_batch(1024)
            record_batch_reader.read_next_batch()

    def test_many_list_batches(self):
        conn = duckdb.connect()

        conn.execute(
            """
            create or replace table tbl as select * from (select {'a': [5,4,3,2,1]}), range(10000000)
        """
        )

        query = "SELECT * FROM tbl"
        chunk_size = 1_000_000

        # Because this produces multiple chunks, this caused a segfault before
        # because we changed some data in the first batch fetch
        batch_iter = conn.execute(query).fetch_record_batch(chunk_size)
        for batch in batch_iter:
            del batch

    def test_many_chunk_sizes(self):
        object_size = 1000000
        duckdb_cursor = duckdb.connect()
        query = duckdb_cursor.execute(f"CREATE table t as select range a from range({object_size});")
        for i in [1, 2, 4, 8, 16, 32, 33, 77, 999, 999999]:
            query = duckdb_cursor.execute("SELECT a FROM t")
            record_batch_reader = query.fetch_record_batch(i)
            num_loops = int(object_size / i)
            for j in range(num_loops):
                assert record_batch_reader.schema.names == ['a']
                chunk = record_batch_reader.read_next_batch()
                assert len(chunk) == i
            remainder = object_size % i
            if remainder > 0:
                chunk = record_batch_reader.read_next_batch()
                assert len(chunk) == remainder
            with pytest.raises(StopIteration):
                chunk = record_batch_reader.read_next_batch()
