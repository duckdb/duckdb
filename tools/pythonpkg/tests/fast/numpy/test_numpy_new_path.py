""" The support for scaning over numpy arrays reuses many codes for pandas.
    Therefore, we only test the new codes and exec paths.
"""

import numpy as np
import duckdb
from datetime import timedelta
import pytest


class TestScanNumpy(object):
    def test_scan_numpy(self, duckdb_cursor):
        z = np.array([1, 2, 3])
        res = duckdb_cursor.sql("select * from z").fetchall()
        assert res == [(1,), (2,), (3,)]

        z = np.array([[1, 2, 3], [4, 5, 6]])
        res = duckdb_cursor.sql("select * from z").fetchall()
        assert res == [(1, 4), (2, 5), (3, 6)]

        z = [np.array([1, 2, 3]), np.array([4, 5, 6])]
        res = duckdb_cursor.sql("select * from z").fetchall()
        assert res == [(1, 4), (2, 5), (3, 6)]

        z = {"z": np.array([1, 2, 3]), "x": np.array([4, 5, 6])}
        res = duckdb_cursor.sql("select * from z").fetchall()
        assert res == [(1, 4), (2, 5), (3, 6)]

        z = np.array(["zzz", "xxx"])
        res = duckdb_cursor.sql("select * from z").fetchall()
        assert res == [('zzz',), ('xxx',)]

        z = [np.array(["zzz", "xxx"]), np.array([1, 2])]
        res = duckdb_cursor.sql("select * from z").fetchall()
        assert res == [('zzz', 1), ('xxx', 2)]

        # test ndarray with dtype = object (python dict)
        z = []
        for i in range(3):
            z.append({str(3 - i): i})
        z = np.array(z)
        res = duckdb_cursor.sql("select * from z").fetchall()
        assert res == [
            ({'3': 0},),
            ({'2': 1},),
            ({'1': 2},),
        ]

        # test timedelta
        delta = timedelta(days=50, seconds=27, microseconds=10, milliseconds=29000, minutes=5, hours=8, weeks=2)
        delta2 = timedelta(days=5, seconds=27, microseconds=10, milliseconds=29000, minutes=5, hours=8, weeks=2)
        z = np.array([delta, delta2])
        res = duckdb_cursor.sql("select * from z").fetchall()
        assert res == [
            (timedelta(days=64, seconds=29156, microseconds=10),),
            (timedelta(days=19, seconds=29156, microseconds=10),),
        ]

        # np.empty
        z = np.empty((3,))
        res = duckdb_cursor.sql("select * from z").fetchall()
        assert len(res) == 3

        # empty list
        z = np.array([])
        res = duckdb_cursor.sql("select * from z").fetchall()
        assert res == []

        # np.nan
        z = np.array([np.nan])
        res = duckdb_cursor.sql("select * from z").fetchall()
        assert res == [(None,)]

        # dict of mixed types
        z = {"z": np.array([1, 2, 3]), "x": np.array(["z", "x", "c"])}
        res = duckdb_cursor.sql("select * from z").fetchall()
        assert res == [(1, 'z'), (2, 'x'), (3, 'c')]

        # list of mixed types
        z = [np.array([1, 2, 3]), np.array(["z", "x", "c"])]
        res = duckdb_cursor.sql("select * from z").fetchall()
        assert res == [(1, 'z'), (2, 'x'), (3, 'c')]

        # currently unsupported formats, will throw duckdb.InvalidInputException

        # list of arrays with different length
        z = [np.array([1, 2]), np.array([3])]
        with pytest.raises(duckdb.InvalidInputException):
            duckdb_cursor.sql("select * from z")

        # dict of ndarrays of different length
        z = {"z": np.array([1, 2]), "x": np.array([3])}
        with pytest.raises(duckdb.InvalidInputException):
            duckdb_cursor.sql("select * from z")

        # high dimensional tensors
        z = np.array([[[1, 2]]])
        with pytest.raises(duckdb.InvalidInputException):
            duckdb_cursor.sql("select * from z")

        # list of ndarrys with len(shape) > 1
        z = [np.array([[1, 2], [3, 4]])]
        with pytest.raises(duckdb.InvalidInputException):
            duckdb_cursor.sql("select * from z")

        # dict of ndarrays with len(shape) > 1
        z = {"x": np.array([[1, 2], [3, 4]])}
        with pytest.raises(duckdb.InvalidInputException):
            duckdb_cursor.sql("select * from z")
