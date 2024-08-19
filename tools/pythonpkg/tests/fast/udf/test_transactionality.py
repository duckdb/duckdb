import duckdb
import pytest


class TestUDFTransactionality(object):
    @pytest.mark.xfail(reason='fetchone() does not realize the stream result was closed before completion')
    def test_type_coverage(self, duckdb_cursor):
        rel = duckdb_cursor.sql("select * from range(4096)")
        res = rel.fetchone()
        assert res == (0,)

        def my_func(x: str) -> int:
            return int(x)

        duckdb_cursor.create_function('test', my_func)

        with pytest.raises(duckdb.InvalidInputException, match='result closed'):
            res = rel.fetchone()
