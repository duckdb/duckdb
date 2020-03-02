# timestamp ms precision

import numpy 


class TestNumpyTimestampMilliseconds(object):
    def test_numpy_timestamp(self, duckdb_cursor):
        res = duckdb_cursor.execute("SELECT TIMESTAMP '2020-01-10 00:00:00.500' as test_time").fetchnumpy()
        assert res['test_time'] == numpy.datetime64('2020-01-10 00:00:00.500')
