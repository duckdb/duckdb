# simple DB API testcase

import numpy
import pytest
import duckdb
from conftest import NumpyPandas, ArrowPandas


def assert_result_equal(result):
    assert result == [(0,), (1,), (2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (None,)], "Incorrect result returned"


class TestSimpleDBAPI(object):
    def test_regular_selection(self, duckdb_cursor, integers):
        duckdb_cursor.execute('SELECT * FROM integers')
        result = duckdb_cursor.fetchall()
        assert_result_equal(result)

    def test_fetchmany_default(self, duckdb_cursor, integers):
        # Get truth-value
        truth_value = len(duckdb_cursor.execute("select * from integers").fetchall())

        duckdb_cursor.execute('Select * from integers')
        # by default 'size' is 1
        arraysize = 1
        list_of_results = []
        while True:
            res = duckdb_cursor.fetchmany()
            assert isinstance(res, list)
            list_of_results.extend(res)
            if len(res) == 0:
                break
        assert len(list_of_results) == truth_value
        assert_result_equal(list_of_results)
        res = duckdb_cursor.fetchmany(2)
        assert len(res) == 0
        res = duckdb_cursor.fetchmany(3)
        assert len(res) == 0

    def test_fetchmany(self, duckdb_cursor, integers):
        # Get truth value
        truth_value = len(duckdb_cursor.execute("select * from integers").fetchall())
        duckdb_cursor.execute('select * from integers')
        list_of_results = []
        arraysize = 3
        expected_iteration_count = 1 + (int)(truth_value / arraysize) + (1 if truth_value % arraysize else 0)
        iteration_count = 0
        print("truth_value:", truth_value)
        print("expected_iteration_count:", expected_iteration_count)
        while True:
            print(iteration_count)
            res = duckdb_cursor.fetchmany(3)
            print(res)
            iteration_count += 1
            assert isinstance(res, list)
            list_of_results.extend(res)
            if len(res) == 0:
                break
        assert iteration_count == expected_iteration_count
        assert len(list_of_results) == truth_value
        assert_result_equal(list_of_results)
        res = duckdb_cursor.fetchmany(3)
        assert len(res) == 0

    def test_fetchmany_too_many(self, duckdb_cursor, integers):
        truth_value = len(duckdb_cursor.execute('select * from integers').fetchall())
        duckdb_cursor.execute('select * from integers')
        res = duckdb_cursor.fetchmany(truth_value * 5)
        assert len(res) == truth_value
        assert_result_equal(res)
        res = duckdb_cursor.fetchmany(2)
        assert len(res) == 0
        res = duckdb_cursor.fetchmany(3)
        assert len(res) == 0

    def test_numpy_selection(self, duckdb_cursor, integers, timestamps):
        duckdb_cursor.execute('SELECT * FROM integers')
        result = duckdb_cursor.fetchnumpy()
        arr = numpy.ma.masked_array(numpy.arange(11))
        arr.mask = [False] * 10 + [True]
        numpy.testing.assert_array_equal(result['i'], arr, "Incorrect result returned")
        duckdb_cursor.execute('SELECT * FROM timestamps')
        result = duckdb_cursor.fetchnumpy()
        arr = numpy.array(['1992-10-03 18:34:45', '2010-01-01 00:00:01', None], dtype="datetime64[ms]")
        arr = numpy.ma.masked_array(arr)
        arr.mask = [False, False, True]
        numpy.testing.assert_array_equal(result['t'], arr, "Incorrect result returned")

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_pandas_selection(self, duckdb_cursor, pandas, integers, timestamps):
        import datetime

        duckdb_cursor.execute('SELECT * FROM integers')
        result = duckdb_cursor.fetchdf()
        arr = numpy.ma.masked_array(numpy.arange(11))
        arr.mask = [False] * 10 + [True]
        arr = {'i': arr}
        arr = pandas.DataFrame(arr)
        pandas.testing.assert_frame_equal(result, arr)

        duckdb_cursor.execute('SELECT * FROM timestamps')
        result = duckdb_cursor.fetchdf()
        df = pandas.DataFrame(
            {
                't': pandas.Series(
                    data=[
                        datetime.datetime(year=1992, month=10, day=3, hour=18, minute=34, second=45),
                        datetime.datetime(year=2010, month=1, day=1, hour=0, minute=0, second=1),
                        None,
                    ],
                    dtype='datetime64[us]',
                )
            }
        )
        pandas.testing.assert_frame_equal(result, df)

    # def test_numpy_creation(self, duckdb_cursor):
    #     # numpyarray = {'i': numpy.arange(10), 'v': numpy.random.randint(100, size=(1, 10))}  # segfaults
    #     data_dict = {'i': numpy.arange(10), 'v': numpy.random.randint(100, size=10)}
    #     duckdb_cursor.create('numpy_creation', data_dict)
    #     duckdb_cursor.commit()

    #     duckdb_cursor.execute('SELECT * FROM numpy_creation')
    #     result = duckdb_cursor.fetchnumpy()

    #     numpy.testing.assert_array_equal(result['i'], data_dict['i'])
    #     numpy.testing.assert_array_equal(result['v'], data_dict['v'])

    # def test_pandas_creation(self, duckdb_cursor):
    #     data_dict = {'i': numpy.arange(10), 'v': numpy.random.randint(100, size=10)}
    #     dframe = pandas.DataFrame.from_dict(data_dict)
    #     duckdb_cursor.create('dframe_creation', dframe)

    #     duckdb_cursor.execute('SELECT * FROM dframe_creation')
    #     result = duckdb_cursor.fetchnumpy()

    #     numpy.testing.assert_array_equal(result['i'], data_dict['i'])
    #     numpy.testing.assert_array_equal(result['v'], data_dict['v'])

    # def test_numpy_insertion(self, duckdb_cursor):
    #     data_dict = {'i': numpy.arange(10), 'v': numpy.random.randint(100, size=10)}
    #     duckdb_cursor.execute("CREATE TABLE numpy_insertion (i INT, v INT)")
    #     duckdb_cursor.insert('numpy_insertion', data_dict)
    #     duckdb_cursor.commit()

    #     duckdb_cursor.execute("SELECT * FROM numpy_insertion")
    #     result = duckdb_cursor.fetchnumpy()

    #     numpy.testing.assert_array_equal(result['i'], data_dict['i'])
    #     numpy.testing.assert_array_equal(result['v'], data_dict['v'])

    # def test_pandas_insertion(self, duckdb_cursor):
    #     data_dict = {'i': numpy.arange(10), 'v': numpy.random.randint(100, size=10)}
    #     dframe = pandas.DataFrame.from_dict(data_dict)
    #     duckdb_cursor.execute("CREATE TABLE pandas_insertion (i INT, v INT)")
    #     duckdb_cursor.insert('pandas_insertion', dframe)
    #     duckdb_cursor.commit()

    #     duckdb_cursor.execute("SELECT * FROM pandas_insertion")
    #     result = duckdb_cursor.fetchnumpy()

    #     numpy.testing.assert_array_equal(result['i'], data_dict['i'])
    #     numpy.testing.assert_array_equal(result['v'], data_dict['v'])

    # def test_masked_array_insertion(self, duckdb_cursor):
    #     data_dict = {'i': numpy.ma.masked_array(numpy.arange(10), mask=([False]*9 + [True]))}
    #     duckdb_cursor.execute("CREATE TABLE masked_array_insertion (i INT)")
    #     duckdb_cursor.insert("masked_array_insertion", data_dict)
    #     duckdb_cursor.commit()

    #     duckdb_cursor.execute("SELECT * FROM masked_array_insertion")
    #     result = duckdb_cursor.fetchnumpy()

    #     numpy.testing.assert_array_equal(result['i'], data_dict['i'])
