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
    result.check_stdout('(SCALAR_FUNCTION)')
    result.check_stdout('Signatures')
    result.check_stdout('Description')
    result.check_stdout('Examples')
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
    result.check_stdout('Signatures')


def test_manual_unknown_function(shell):
    test = (
        ShellTest(shell)
        .statement('.manual this_function_does_not_exist')
    )
    result = test.run()
    result.check_stderr('No function named')
