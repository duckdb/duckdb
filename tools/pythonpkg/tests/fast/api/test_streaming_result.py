import pytest
import duckdb


class TestStreamingResult(object):
    def test_fetch_one(self, duckdb_cursor):
        # fetch one
        res = duckdb_cursor.sql('SELECT * FROM range(100000)')
        result = []
        while len(result) < 5000:
            tpl = res.fetchone()
            result.append(tpl[0])
        assert result == list(range(5000))

        # fetch one with error
        res = duckdb_cursor.sql(
            "SELECT CASE WHEN i < 10000 THEN i ELSE concat('hello', i::VARCHAR)::INT END FROM range(100000) t(i)"
        )
        with pytest.raises(duckdb.ConversionException):
            while True:
                tpl = res.fetchone()
                if tpl is None:
                    break

    def test_fetch_many(self, duckdb_cursor):
        # fetch many
        res = duckdb_cursor.sql('SELECT * FROM range(100000)')
        result = []
        while len(result) < 5000:
            tpl = res.fetchmany(10)
            result += [x[0] for x in tpl]
        assert result == list(range(5000))

        # fetch many with error
        res = duckdb_cursor.sql(
            "SELECT CASE WHEN i < 10000 THEN i ELSE concat('hello', i::VARCHAR)::INT END FROM range(100000) t(i)"
        )
        with pytest.raises(duckdb.ConversionException):
            while True:
                tpl = res.fetchmany(10)
                if tpl is None:
                    break

    def test_record_batch_reader(self, duckdb_cursor):
        pytest.importorskip("pyarrow")
        pytest.importorskip("pyarrow.dataset")
        # record batch reader
        res = duckdb_cursor.sql('SELECT * FROM range(100000) t(i)')
        reader = res.fetch_arrow_reader(batch_size=16_384)
        result = []
        for batch in reader:
            result += batch.to_pydict()['i']
        assert result == list(range(100000))

        # record batch reader with error
        res = duckdb_cursor.sql(
            "SELECT CASE WHEN i < 10000 THEN i ELSE concat('hello', i::VARCHAR)::INT END FROM range(100000) t(i)"
        )
        with pytest.raises(duckdb.ConversionException, match="Could not convert string 'hello10000' to INT32"):
            reader = res.fetch_arrow_reader(batch_size=16_384)

    def test_9801(self, duckdb_cursor):
        duckdb_cursor.execute('CREATE TABLE test(id INTEGER , name VARCHAR NOT NULL);')

        words = ['aaaaaaaaaaaaaaaaaaaaaaa', 'bbbb', 'ccccccccc', 'ííííííííí']
        lines = [(i, words[i % 4]) for i in range(1000)]
        duckdb_cursor.executemany("INSERT INTO TEST (id, name) VALUES (?, ?)", lines)

        rel1 = duckdb_cursor.sql(
            """
            SELECT id, name FROM test ORDER BY id ASC
        """
        )
        result = rel1.fetchmany(size=5)
        counter = 0
        while result != []:
            for x in result:
                assert x == (counter, words[counter % 4])
                counter += 1
            result = rel1.fetchmany(size=5)
