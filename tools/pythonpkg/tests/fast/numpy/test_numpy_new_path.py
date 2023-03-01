""" The support for scaning over numpy arrays reuses many codes for pandas.
    Therefore, we only test the new codes and exec paths.
"""

import numpy as np
import duckdb

class TestScanNumpy(object):
    def test_scan_numpy(self, duckdb_cursor):
        z = np.array([1,2,3])
        res = duckdb.sql("select * from z").fetchall()
        assert res == [(1,), (2,), (3,)]

        z = np.array([[1,2,3], [4,5,6]])
        res = duckdb.sql("select * from z").fetchall()
        assert res == [(1, 4), (2, 5), (3, 6)]

        z = [np.array([1,2,3]), np.array([4,5,6])]
        res = duckdb.sql("select * from z").fetchall()
        assert res == [(1, 4), (2, 5), (3, 6)]

        z = {"z": np.array([1,2,3]), "x": np.array([4,5,6])}
        res = duckdb.sql("select * from z").fetchall()
        assert res == [(1, 4), (2, 5), (3, 6)]
