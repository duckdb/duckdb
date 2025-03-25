import duckdb
import os
import pytest

pd = pytest.importorskip("pandas")
pa = pytest.importorskip("pyarrow")
from typing import Union
import pyarrow.compute as pc
import uuid
import datetime
import numpy as np
import cmath

from duckdb.typing import *


class TestRemoveFunction(object):
    def test_not_created(self):
        con = duckdb.connect()
        with pytest.raises(
            duckdb.InvalidInputException,
            match="No function by the name of 'not_a_registered_function' was found in the list of registered functions",
        ):
            con.remove_function('not_a_registered_function')

    def test_double_remove(self):
        def func(x: int) -> int:
            return x

        con = duckdb.connect()
        con.create_function('func', func)
        con.sql('select func(42)')
        con.remove_function('func')
        with pytest.raises(
            duckdb.InvalidInputException,
            match="No function by the name of 'func' was found in the list of registered functions",
        ):
            con.remove_function('func')

        with pytest.raises(duckdb.CatalogException, match='Scalar Function with name func does not exist!'):
            con.sql('select func(42)')

    def test_use_after_remove(self):
        def func(x: int) -> int:
            return x

        con = duckdb.connect()
        con.create_function('func', func)
        rel = con.sql('select func(42)')
        con.remove_function('func')
        """
            Error: Catalog Error: Scalar Function with name func does not exist!
        """
        with pytest.raises(
            duckdb.InvalidInputException, match='Attempting to execute an unsuccessful or closed pending query result'
        ):
            res = rel.fetchall()

    def test_use_after_remove_and_recreation(self):
        def func(x: str) -> str:
            return x

        con = duckdb.connect()
        con.create_function('func', func)

        with pytest.raises(duckdb.BinderException, match='No function matches the given name'):
            rel1 = con.sql('select func(42)')
        rel2 = con.sql("select func('test'::VARCHAR)")
        con.remove_function('func')

        def also_func(x: int) -> int:
            return x

        con.create_function('func', also_func)
        with pytest.raises(duckdb.InvalidInputException, match='No function matches the given name'):
            res = rel2.fetchall()

    def test_overwrite_name(self):
        def func(x):
            return x

        con = duckdb.connect()
        # create first version of the function
        con.create_function('func', func, [BIGINT], BIGINT)

        # create relation that uses the function
        rel1 = con.sql("select func('3')")

        def other_func(x):
            return x

        with pytest.raises(
            duckdb.NotImplementedException,
            match="A function by the name of 'func' is already created, creating multiple functions with the same name is not supported yet, please remove it first",
        ):
            con.create_function('func', other_func, [VARCHAR], VARCHAR)

        con.remove_function('func')

        with pytest.raises(
            duckdb.InvalidInputException, match='Catalog Error: Scalar Function with name func does not exist!'
        ):
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
