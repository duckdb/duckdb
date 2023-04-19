import duckdb
import os
import pandas as pd
import pytest

from duckdb.typing import *

class TestScalarUDF(object):
    def test_basic_use(self):
        def plus_one(x):
            if x == None or x > 50:
                return x;
            return x + 1

        con = duckdb.connect()
        con.register_scalar('plus_one', plus_one, [BIGINT], BIGINT)
        assert [(6,)] == con.sql('select plus_one(5)').fetchall()

        range_table = con.table_function('range', [5000])
        res = con.sql('select plus_one(i) from range_table tbl(i)').fetchall()
        assert len(res) == 5000

        # FIXME: this is dependent on the duckdb vector size
        # which we can get through `duckdb.__standard_vector_size__`
        res = con.sql('select i, plus_one(i) from test_vector_types(NULL::BIGINT, false) t(i), range(2000)')
        assert len(res) == 22000

    def test_detected_parameters(self):
        def concatenate(a: str, b: str):
            return a + b
        
        con = duckdb.connect()
        con.register_scalar('py_concatenate', concatenate, None, VARCHAR)
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
        con.register_scalar('add_nums', add_nums)
        res = con.sql("""
            select add_nums(5,3,2,1);
        """).fetchall()
        assert res[0][0] == 11

    def test_varargs(self):
        def variable_args(*args):
            amount = len(args)
            return amount
        
        con = duckdb.connect()
        con.register_scalar('varargs', variable_args, None, BIGINT, varargs=True)
        res = con.sql("""select varargs('5', '3', '2', 1, 0.12345)""").fetchall()
        assert res == [(5,)]

    def test_overwrite_name(self):
        def func(x):
            return x
        # TODO: test proper behavior when you register two functions with the same name
        con = duckdb.connect()
        # create first version of the function
        con.register_scalar('func', func, [BIGINT], BIGINT)

        # create relation that uses the function
        rel1 = con.sql('select func(3)')

        def other_func(x):
            return x
        # create second version of the function
        with pytest.raises(duckdb.CatalogException, match="""Catalog Error: Scalar Function with name "func" already exists!"""):
            con.register_scalar('func', other_func, [VARCHAR], VARCHAR)
        return

        # create relation that uses the new version
        rel2 = con.sql("select func('test')")

        # execute both relations
        res1 = rel1.fetchall()
        res2 = rel2.fetchall()
        print(res1)
        print(res2)
        pass

    def test_nulls(self):
        def five_if_null(x):
            if (x == None):
                return 5
            return x
        con = duckdb.connect()
        con.register_scalar('null_test', five_if_null, [BIGINT], BIGINT, null_handling = "SPECIAL")
        res = con.sql('select null_test(NULL)').fetchall()
        assert res == [(5,)]

    def test_exceptions(self):
        def throws(x):
            raise AttributeError("test")

        con = duckdb.connect()
        con.register_scalar('forward_error', throws, [BIGINT], BIGINT, exception_handling='default')
        res = con.sql('select forward_error(5)')
        with pytest.raises(duckdb.InvalidInputException, match='Python exception occurred while executing the UDF'):
            res.fetchall()
        
        con.register_scalar('return_null', throws, [BIGINT], BIGINT, exception_handling='return_null')
        res = con.sql('select return_null(5)').fetchall()
        assert res == [(None,)]

    def test_structs(self):
        def add_extra_column(original):
            original['a'] = 200
            original['bb'] = 0
            return original

        con = duckdb.connect()
        range_table = con.table_function('range', [5000])
        con.register_scalar("append_field", add_extra_column, [duckdb.struct_type({'a': BIGINT, 'b': BIGINT})], duckdb.struct_type({'a': BIGINT, 'b': BIGINT, 'c': BIGINT}))

        res = con.sql("""
            select append_field({'a': i::BIGINT, 'b': 3::BIGINT}) from range_table tbl(i)
        """)
        # added extra column to the struct
        assert len(res.fetchone()[0].keys()) == 3
        # FIXME: this is needed, otherwise the old transaction is still active when we try to start a new transaction inside of 'register_scalar', which means the call would fail
        res.fetchall()

        def swap_keys(dict):
            result = {}
            reversed_keys = list(dict.keys())
            reversed_keys.reverse()
            for item in reversed_keys:
                result[item] = dict[item]
            return result
        
        con.register_scalar('swap_keys', swap_keys, [con.struct_type({'a': BIGINT, 'b': VARCHAR})], con.struct_type({'a': VARCHAR, 'b': BIGINT}))
        res = con.sql("""
        select swap_keys({'a': 42, 'b': 'answer_to_life'})
        """).fetchall()
        assert res == [({'a': 'answer_to_life', 'b': 42},)]
