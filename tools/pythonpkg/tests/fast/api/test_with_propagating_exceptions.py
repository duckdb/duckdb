import pytest
import duckdb


class TestWithPropagatingExceptions(object):
    def test_with(self):
        # Should propagate exception raised in the 'with duckdb.connect() ..'
        with pytest.raises(duckdb.ParserException, match="syntax error at or near *"):
            with duckdb.connect() as con:
                print('before')
                con.execute('invalid')
                print('after')

        # Does not raise an exception
        with duckdb.connect() as con:
            print('before')
            con.execute('select 1')
            print('after')
