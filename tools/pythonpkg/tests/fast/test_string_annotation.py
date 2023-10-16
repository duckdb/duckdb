import duckdb
import os
import pandas as pd
import pytest
from typing import Union

# This is used to opt in to PEP 563 (string annotations)
from __future__ import annotations


def make_annotated_function(type):
    # Create a function that returns its input
    def test_base(x):
        return x

    import types

    test_function = types.FunctionType(
        test_base.__code__, test_base.__globals__, test_base.__name__, test_base.__defaults__, test_base.__closure__
    )
    # Add annotations for the return type and 'x'
    test_function.__annotations__ = {'return': type, 'x': type}
    return test_function

class TestStringAnnotation(object):
    def test_string_annotations(self):
        pass
