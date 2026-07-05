# fmt: off

import pytest
from conftest import ShellTest


def test_manual_basic(shell):
    # a scalar function with a single description and example
    test = (
        ShellTest(shell)
        .statement('.manual list_value')
    )
    result = test.run()
    result.check_stdout('list_value')
    result.check_stdout('SCALAR FUNCTIONS')
    result.check_stdout('DESCRIPTIONS')
    result.check_stdout('EXAMPLES')
    result.check_stdout('->')


def test_manual_multiple_overloads(shell):
    # multiple overloads get numbered signatures
    test = (
        ShellTest(shell)
        .statement('.manual regexp_matches')
    )
    result = test.run()
    result.check_stdout('(1)')
    result.check_stdout('(2)')


def test_manual_dedup_description(shell):
    # overloads sharing a description are grouped under one reference header
    test = (
        ShellTest(shell)
        .statement('.manual regexp_extract')
    )
    result = test.run()
    # regexp_extract has two distinct descriptions grouped across overloads
    result.check_stdout('(1), (2), (4)')
    result.check_stdout('(3), (5)')


def test_manual_case_insensitive(shell):
    test = (
        ShellTest(shell)
        .statement('.manual LIST_VALUE')
    )
    result = test.run()
    result.check_stdout('SCALAR FUNCTIONS')


def test_manual_multiple_function_types(shell):
    # a name with both scalar and table overloads gets a heading per type, numbered continuously
    test = (
        ShellTest(shell)
        .statement('.manual generate_series')
    )
    result = test.run()
    result.check_stdout('SCALAR FUNCTIONS')
    result.check_stdout('TABLE FUNCTIONS')


def test_manual_type(shell):
    # a type is listed under a TYPES heading; a single entry gets no signature number
    test = (
        ShellTest(shell)
        .statement('.manual VARCHAR')
    )
    result = test.run()
    result.check_stdout('TYPES')
    result.check_stdout('VARCHAR')
    result.check_not_exist('(1)')


def test_manual_type_and_function(shell):
    # "list" is both the LIST type and the list() aggregate - show both sections
    test = (
        ShellTest(shell)
        .statement('.manual list')
    )
    result = test.run()
    result.check_stdout('AGGREGATE FUNCTIONS')
    result.check_stdout('TYPES')


def test_manual_unknown_function(shell):
    test = (
        ShellTest(shell)
        .statement('.manual this_function_does_not_exist')
    )
    result = test.run()
    result.check_stderr('No function or type named')


def test_manual_did_you_mean(shell):
    # a near-miss name suggests the closest matches
    test = (
        ShellTest(shell)
        .statement('.manual reverze')
    )
    result = test.run()
    result.check_stderr('Did you mean')
    result.check_stderr('reverse')
