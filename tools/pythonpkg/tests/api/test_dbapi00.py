# simple DB API testcase

import numpy
import pandas


class TestSimpleDBAPI(object):
    def test_regular_selection(self, duckdb_cursor):
        duckdb_cursor.execute('SELECT * FROM integers')
        result = duckdb_cursor.fetchall()
        assert result == [(0,), (1,), (2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (None,)], "Incorrect result returned"

    def test_numpy_selection(self, duckdb_cursor):
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

    def test_pandas_selection(self, duckdb_cursor):
        duckdb_cursor.execute('SELECT * FROM integers')
        result = duckdb_cursor.fetchdf()
        arr = numpy.ma.masked_array(numpy.arange(11))
        arr.mask = [False] * 10 + [True]
        arr = {'i': arr}
        arr = pandas.DataFrame.from_dict(arr)
        pandas.testing.assert_frame_equal(result, arr)

        duckdb_cursor.execute('SELECT * FROM timestamps')
        result = duckdb_cursor.fetchdf()
        df = pandas.DataFrame({
            't': pandas.to_datetime(['1992-10-03 18:34:45', '2010-01-01 00:00:01', None])
        })
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
