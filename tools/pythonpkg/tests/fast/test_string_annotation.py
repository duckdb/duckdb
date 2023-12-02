import duckdb
import pytest
import sys
from typing import Union


def make_annotated_function(type: str):
    def test_base():
        return None

    import types

    test_function = types.FunctionType(
        test_base.__code__, test_base.__globals__, test_base.__name__, test_base.__defaults__, test_base.__closure__
    )
    # Add the 'type' string as return_annotation
    test_function.__annotations__ = {'return': type}
    return test_function


def python_version_lower_than_3_10():
    import sys

    if sys.version_info[0] < 3:
        return True
    if sys.version_info[1] < 10:
        return True
    return False


class TestStringAnnotation(object):
    @pytest.mark.skipif(
        python_version_lower_than_3_10(), reason="inspect.signature(eval_str=True) only supported since 3.10 and higher"
    )
    @pytest.mark.parametrize(
        ['input', 'expected'],
        [
            ('str', 'VARCHAR'),
            ('list[str]', 'VARCHAR[]'),
            ('dict[str, str]', 'MAP(VARCHAR, VARCHAR)'),
            ('dict[Union[str, bool], str]', 'MAP(UNION(u1 VARCHAR, u2 BOOLEAN), VARCHAR)'),
        ],
    )
    def test_string_annotations(self, duckdb_cursor, input, expected):
        from inspect import signature

        func = make_annotated_function(input)
        sig = signature(func)
        assert sig.return_annotation.__class__ == str

        duckdb_cursor.create_function("foo", func)
        rel = duckdb_cursor.sql("select foo()")
        assert rel.types == [expected]
