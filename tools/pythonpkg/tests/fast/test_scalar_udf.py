from sqlalchemy import VARCHAR
import duckdb
import os
import pandas as pd
import pytest
from typing import Union

from duckdb.typing import *

class TestScalarUDF(object):
    def test_basic_use(self, duckdb_cursor):
        def plus_one(x):
            if x == None or x > 50:
                return x;
            return x + 1

        con = duckdb_cursor
        con.register_scalar('plus_one', plus_one, [BIGINT], BIGINT)
        assert [(6,)] == con.sql('select plus_one(5)').fetchall()

        range_table = con.table_function('range', [5000])
        res = con.sql('select plus_one(i) from range_table tbl(i)').fetchall()
        assert len(res) == 5000

        # FIXME: this is dependent on the duckdb vector size
        # which we can get through `duckdb.__standard_vector_size__`
        res = con.sql('select i, plus_one(i) from test_vector_types(NULL::BIGINT, false) t(i), range(2000)')
        assert len(res) == 22000

    def test_varargs(self, duckdb_cursor):
        def variable_args(*args):
            amount = len(args)
            return amount
        
        con = duckdb_cursor
        con.register_scalar('varargs', variable_args, None, BIGINT, varargs=True)
        res = con.sql("""select varargs('5', '3', '2', 1, 0.12345)""").fetchall()
        assert res == [(5,)]

    def test_overwrite_name(self, duckdb_cursor):
        # TODO: test proper behavior when you register two functions with the same name
        
        # create first version of the function

        # create relation that uses the function

        # create second version of the function

        # create relation that uses the new version

        # execute both relations
        pass

    def test_nulls(self, duckdb_cursor):
        # TODO: provide 'null_handling' option?
        pass
    
    def test_exceptions(self, duckdb_cursor):
        # TODO: we likely want an enum to define how exceptions should be handled
        # - propagate:
        #    throw the exception as a duckdb exception
        # - ignore:
        #    return NULL instead
        pass

    def test_binding(self, duckdb_cursor):
        # TODO: add a way to do extra binding for the UDF
        pass
    
    def test_structs(self, duckdb_cursor):
        def add_extra_column(original):
            original['a'] = 200
            original['bb'] = 0
            return original

        con = duckdb_cursor
        range_table = con.table_function('range', [5000])
        con.register_scalar("append_field", add_extra_column, [duckdb.struct_type({'a': BIGINT, 'b': BIGINT})], duckdb.struct_type({'a': BIGINT, 'b': BIGINT, 'c': BIGINT}))

        res = con.sql("""
            select append_field({'a': i::BIGINT, 'b': 3::BIGINT}) from range_table tbl(i)
        """)
        # added extra column to the struct
        assert len(res.fetchone()[0].keys()) == 3
        # FIXME: this is needed, otherwise the transaction is still active in 'register_scalar' and we can't begin a new one
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
