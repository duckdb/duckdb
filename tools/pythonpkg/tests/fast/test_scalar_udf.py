import duckdb
import os
import pandas as pd
import pytest

from duckdb.typing import *

class TestScalarUDF(object):
    def test_default_conn(self):
        def passthrough(x):
            return x
        
        duckdb.create_function('default_conn_passthrough', passthrough, [BIGINT], BIGINT)
        res = duckdb.sql('select default_conn_passthrough(5)').fetchall()
        assert res == [(5,)]

    def test_basic_use(self):
        def plus_one(x):
            if x == None or x > 50:
                return x;
            return x + 1

        con = duckdb.connect()
        con.create_function('plus_one', plus_one, [BIGINT], BIGINT)
        assert [(6,)] == con.sql('select plus_one(5)').fetchall()

        range_table = con.table_function('range', [5000])
        res = con.sql('select plus_one(i) from range_table tbl(i)').fetchall()
        assert len(res) == 5000

        # FIXME: this is dependent on the duckdb vector size
        # which we can get through `duckdb.__standard_vector_size__`
        res = con.sql('select i, plus_one(i) from test_vector_types(NULL::BIGINT, false) t(i), range(2000)')
        assert len(res) == 22000

    def test_passthrough(self):
        def passthrough(x):
            return x

        con = duckdb.connect()
        con.create_function('passthrough', passthrough, [BIGINT], BIGINT)
        assert con.sql('select passthrough(i) from range(5000) tbl(i)').fetchall() == con.sql('select * from range(5000)').fetchall()

    def test_execute(self):
        def func(x):
            return x % 2

        con = duckdb.connect()
        con.create_function('modulo_op', func, [BIGINT], TINYINT)
        res = con.execute('select modulo_op(?)', [5]).fetchall()
        assert res == [(1,)]

    def test_cast_output(self):
        def takes_string(x):
            return x

        con = duckdb.connect()
        con.create_function('casts_from_string', takes_string, [VARCHAR], BIGINT)

        res = con.sql("select casts_from_string('42')").fetchall()
        assert res == [(42,)]

        with pytest.raises(duckdb.InvalidInputException):
            res = con.sql("select casts_from_string('test')").fetchall()

    def test_detected_parameters(self):
        def concatenate(a: str, b: str):
            return a + b

        con = duckdb.connect()
        con.create_function('py_concatenate', concatenate, None, VARCHAR)
        res = con.sql("""
            select py_concatenate('5','3');
        """).fetchall()
        assert res[0][0] == '53'

    def test_detected_return_type(self):
        def add_nums(*args) -> int:
            sum = 0;
            for arg in args:
                sum += arg
            return sum

        con = duckdb.connect()
        con.create_function('add_nums', add_nums)
        res = con.sql("""
            select add_nums(5,3,2,1);
        """).fetchall()
        assert res[0][0] == 11

    def test_varargs(self):
        def variable_args(*args):
            amount = len(args)
            return amount

        con = duckdb.connect()
        con.create_function('varargs', variable_args, None, BIGINT)
        res = con.sql("""select varargs('5', '3', '2', 1, 0.12345)""").fetchall()
        assert res == [(5,)]

    def test_overwrite_name(self):
        def func(x):
            return x
        con = duckdb.connect()
        # create first version of the function
        con.create_function('func', func, [BIGINT], BIGINT)

        # create relation that uses the function
        rel1 = con.sql('select func(3)')

        def other_func(x):
            return x

        with pytest.raises(duckdb.NotImplementedException, match="A function by the name of 'func' is already created, creating multiple functions with the same name is not supported yet, please remove it first"):
            con.create_function('func', other_func, [VARCHAR], VARCHAR)

        con.remove_function('func')

        with pytest.raises(duckdb.InvalidInputException, match='Catalog Error: Scalar Function with name func does not exist!'):
            # Attempted to execute the relation using the 'func' function, but it was deleted
            rel1.fetchall()

        con.create_function('func', other_func, [VARCHAR], VARCHAR)
        # create relation that uses the new version
        rel2 = con.sql("select func('test')")

        # execute both relations
        res1 = rel1.fetchall()
        res2 = rel2.fetchall()
        # This has been converted to string, because the previous version of the function no longer exists
        assert res1 == [('3',)]
        assert res2 == [('test',)]

    def test_nulls(self):
        def five_if_null(x):
            if (x == None):
                return 5
            return x
        con = duckdb.connect()
        con.create_function('null_test', five_if_null, [BIGINT], BIGINT, null_handling = "SPECIAL")
        res = con.sql('select null_test(NULL)').fetchall()
        assert res == [(5,)]

    def test_exceptions(self):
        def throws(x):
            raise AttributeError("test")

        con = duckdb.connect()
        con.create_function('forward_error', throws, [BIGINT], BIGINT, exception_handling='default')
        res = con.sql('select forward_error(5)')
        with pytest.raises(duckdb.InvalidInputException, match='Python exception occurred while executing the UDF'):
            res.fetchall()

        con.create_function('return_null', throws, [BIGINT], BIGINT, exception_handling='return_null')
        res = con.sql('select return_null(5)').fetchall()
        assert res == [(None,)]

    def test_non_callable(self):
        con = duckdb.connect()
        with pytest.raises(TypeError):
            con.create_function('func', 5, [BIGINT], BIGINT)

        class MyCallable:
            def __init__(self):
                pass

            def __call__(self, x):
                return x

        my_callable = MyCallable()
        con.create_function('func', my_callable, [BIGINT], BIGINT)
        res = con.sql('select func(5)').fetchall()
        assert res == [(5,)]

    def test_structs(self):
        def add_extra_column(original):
            original['a'] = 200
            original['bb'] = 0
            return original

        con = duckdb.connect()
        range_table = con.table_function('range', [5000])
        con.create_function("append_field", add_extra_column, [duckdb.struct_type({'a': BIGINT, 'b': BIGINT})], duckdb.struct_type({'a': BIGINT, 'b': BIGINT, 'c': BIGINT}))

        res = con.sql("""
            select append_field({'a': i::BIGINT, 'b': 3::BIGINT}) from range_table tbl(i)
        """)
        # added extra column to the struct
        assert len(res.fetchone()[0].keys()) == 3
        # FIXME: this is needed, otherwise the old transaction is still active when we try to start a new transaction inside of 'create_function', which means the call would fail
        res.fetchall()

        def swap_keys(dict):
            result = {}
            reversed_keys = list(dict.keys())
            reversed_keys.reverse()
            for item in reversed_keys:
                result[item] = dict[item]
            return result

        con.create_function('swap_keys', swap_keys, [con.struct_type({'a': BIGINT, 'b': VARCHAR})], con.struct_type({'a': VARCHAR, 'b': BIGINT}))
        res = con.sql("""
        select swap_keys({'a': 42, 'b': 'answer_to_life'})
        """).fetchall()
        assert res == [({'a': 'answer_to_life', 'b': 42},)]
