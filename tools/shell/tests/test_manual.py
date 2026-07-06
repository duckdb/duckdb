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
    result.check_stdout('scalar function')
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
    result.check_stdout('1.  regexp_extract')
    result.check_stdout('2.  regexp_extract')


def test_manual_shared_content_no_numbers(shell):
    # when every overload shares the same description and example, the numbers/refs are omitted
    test = (
        ShellTest(shell)
        .statement('.manual date_diff')
    )
    result = test.run()
    result.check_not_exist('1. date_diff')


def test_manual_dedup_description(shell):
    # overloads sharing a description are grouped under one reference list
    test = (
        ShellTest(shell)
        .statement('.manual regexp_extract')
    )
    result = test.run()
    # regexp_extract has two distinct descriptions grouped across overloads
    result.check_stdout('1. 2. 4.')
    result.check_stdout('3. 5.')


def test_manual_case_insensitive(shell):
    test = (
        ShellTest(shell)
        .statement('.manual LIST_VALUE')
    )
    result = test.run()
    result.check_stdout('scalar function')


def test_manual_multiple_function_types(shell):
    # a name with both scalar and table overloads gets a separate entry per type
    test = (
        ShellTest(shell)
        .statement('.manual generate_series')
    )
    result = test.run()
    result.check_stdout('scalar function')
    result.check_stdout('table function')


def test_manual_unknown_function(shell):
    test = (
        ShellTest(shell)
        .statement('.manual this_function_does_not_exist')
    )
    result = test.run()
    result.check_stderr('No function matches')


def test_manual_exact_match(shell):
    # a plain argument (no wildcards) matches the function name exactly
    test = (
        ShellTest(shell)
        .statement('.manual list_append')
    )
    result = test.run()
    result.check_stdout('list_append')
    # a substring-only match must NOT be surfaced without an explicit wildcard
    result.check_not_exist('list_apply')


def test_manual_like_pattern_multiple(shell):
    # an explicit wildcard pattern matches multiple names, each with a "n / total" position counter
    test = (
        ShellTest(shell)
        .statement('.manual list_app%')
    )
    result = test.run()
    result.check_stdout('list_append')
    result.check_stdout('list_apply')
    result.check_stdout('1 / ')


def test_manual_single_entry_no_counter(shell):
    # a single entry shows its header (name, schema, type) but no "n / total" position counter
    test = (
        ShellTest(shell)
        .statement('.manual list_contains')
    )
    result = test.run()
    result.check_stdout('list_contains')
    result.check_stdout('scalar function')
    result.check_not_exist(' / ')


def test_manual_schema_path(shell):
    # each signature group is labelled with its qualified schema path
    test = (
        ShellTest(shell)
        .statement('.manual list_contains')
    )
    result = test.run()
    result.check_stdout('system.main')


def test_manual_qualified_schema(shell):
    # a schema-qualified name restricts the match to that schema
    test = (
        ShellTest(shell)
        .statement('.manual main.list_contains')
    )
    result = test.run()
    result.check_stdout('list_contains')
    result.check_stdout('system.main')


def test_manual_qualified_database(shell):
    # a fully qualified database.schema.name is honored
    test = (
        ShellTest(shell)
        .statement('.manual system.main.list_contains')
    )
    result = test.run()
    result.check_stdout('list_contains')
    result.check_stdout('system.main')


def test_manual_qualified_miss(shell):
    # a qualifier that matches no schema yields a miss, not a cross-schema match
    test = (
        ShellTest(shell)
        .statement('.manual not_a_schema.list_contains')
    )
    result = test.run()
    result.check_stderr("No function matches 'not_a_schema.list_contains'")


def test_manual_too_many_qualifiers(shell):
    # more than database.schema.function is rejected
    test = (
        ShellTest(shell)
        .statement('.manual a.b.c.d')
    )
    result = test.run()
    result.check_stderr('is not a valid function name')


def test_manual_quoted_name(shell):
    # quoted components follow the standard qualified-name parsing
    test = ShellTest(shell, ['-manual', '"list_contains"'])
    result = test.run()
    result.check_stdout('list_contains')
    result.check_stdout('scalar function')


def test_manual_did_you_mean(shell):
    # a near-miss name suggests the closest matches
    test = (
        ShellTest(shell)
        .statement('.manual reverze')
    )
    result = test.run()
    result.check_stderr('Did you mean')
    result.check_stderr('reverse')


def test_manual_cli_flag(shell):
    # the -manual CLI flag renders a function's manual page directly and exits
    test = ShellTest(shell, ['-manual', 'list_contains'])
    result = test.run()
    result.check_stdout('list_contains')
    result.check_stdout('scalar function')
    result.check_stdout('DESCRIPTION')


def test_manual_cli_flag_miss(shell):
    # a miss on the CLI flag still reports the error and suggestions
    test = ShellTest(shell, ['-manual', 'reverze'])
    result = test.run()
    result.check_stderr("No function matches 'reverze'")
    result.check_stderr('reverse')
