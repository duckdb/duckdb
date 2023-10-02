import pytest
import duckdb


class TestStreamingResult(object):
    def test_fetch_one(self):
        # fetch one
        res = duckdb.sql('SELECT * FROM range(100000)')
        result = []
        while len(result) < 5000:
            tpl = res.fetchone()
            result.append(tpl[0])
        assert result == list(range(5000))

        # fetch one with error
        res = duckdb.sql(
            "SELECT CASE WHEN i < 10000 THEN i ELSE concat('hello', i::VARCHAR)::INT END FROM range(100000) t(i)"
        )
        with pytest.raises(duckdb.ConversionException):
            while True:
                tpl = res.fetchone()
                if tpl is None:
                    break

    def test_fetch_many(self):
        # fetch many
        res = duckdb.sql('SELECT * FROM range(100000)')
        result = []
        while len(result) < 5000:
            tpl = res.fetchmany(10)
            result += [x[0] for x in tpl]
        assert result == list(range(5000))

        # fetch many with error
        res = duckdb.sql(
            "SELECT CASE WHEN i < 10000 THEN i ELSE concat('hello', i::VARCHAR)::INT END FROM range(100000) t(i)"
        )
        with pytest.raises(duckdb.ConversionException):
            while True:
                tpl = res.fetchmany(10)
                if tpl is None:
                    break

    def test_record_batch_reader(self):
        pytest.importorskip("pyarrow")
        pytest.importorskip("pyarrow.dataset")
        # record batch reader
        res = duckdb.sql('SELECT * FROM range(100000) t(i)')
        reader = res.fetch_arrow_reader(batch_size=16_384)
        result = []
        for batch in reader:
            result += batch.to_pydict()['i']
        assert result == list(range(100000))

        # record batch reader with error
        res = duckdb.sql(
            "SELECT CASE WHEN i < 10000 THEN i ELSE concat('hello', i::VARCHAR)::INT END FROM range(100000) t(i)"
        )
        reader = res.fetch_arrow_reader(batch_size=16_384)
        with pytest.raises(OSError):
            result = []
            for batch in reader:
                result += batch.to_pydict()['i']
