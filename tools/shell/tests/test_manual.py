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
    result.check_stdout('FUNCTIONS (SCALAR)')
    result.check_stdout('DESCRIPTION')
    result.check_stdout('EXAMPLES')
    result.check_stdout('->')


def test_manual_multiple_overloads(shell):
    # overloads get numbered signatures when descriptions/examples differ between them
    test = (
        ShellTest(shell)
        .statement('.manual regexp_extract')
    )
    result = test.run()
    result.check_stdout('(1)')
    result.check_stdout('(2)')


def test_manual_shared_content_no_numbers(shell):
    # when every overload shares the same description and example, the numbers/refs are omitted
    test = (
        ShellTest(shell)
        .statement('.manual date_diff')
    )
    result = test.run()
    result.check_not_exist('(1)')


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
    result.check_stdout('FUNCTIONS (SCALAR)')


def test_manual_multiple_function_types(shell):
    # a name with both scalar and table overloads gets a heading per type, numbered continuously
    test = (
        ShellTest(shell)
        .statement('.manual generate_series')
    )
    result = test.run()
    result.check_stdout('FUNCTIONS (SCALAR)')
    result.check_stdout('FUNCTIONS (TABLE)')


def test_manual_unknown_function(shell):
    test = (
        ShellTest(shell)
        .statement('.manual this_function_does_not_exist')
    )
    result = test.run()
    result.check_stderr('No function named')


def test_manual_banner(shell):
    # the page opens with a banner carrying the entry name, framed by horizontal rules
    test = (
        ShellTest(shell)
        .statement('.manual list_contains')
    )
    result = test.run()
    result.check_stdout('list_contains')
    result.check_stdout('───')


def test_manual_schema_path(shell):
    # each signature group is labelled with its qualified schema path
    test = (
        ShellTest(shell)
        .statement('.manual list_contains')
    )
    result = test.run()
    result.check_stdout('system.main')


def test_manual_did_you_mean(shell):
    # a near-miss name suggests the closest matches
    test = (
        ShellTest(shell)
        .statement('.manual reverze')
    )
    result = test.run()
    result.check_stderr('Did you mean')
    result.check_stderr('reverse')
