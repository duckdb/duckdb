# fmt: off

import pytest
from conftest import ShellTest

def test_pager_status_default(shell):
    """Test that pager status shows 'automatic' by default"""
    test = (
        ShellTest(shell)
            .statement('.pager')
    )
    result = test.run()
    result.check_stdout('Pager mode: automatic')


def test_pager_help(shell):
    """Test that .help shows pager documentation"""
    test = (
        ShellTest(shell)
            .statement('.help pager')
    )
    result = test.run()
    result.check_stdout('DUCKDB_PAGER')


def test_pager_off_explicit(shell):
    """Test setting pager explicitly to off"""
    test = (
        ShellTest(shell)
            .statement('.pager off')
            .statement('.pager')
    )
    result = test.run()
    result.check_stdout('Pager mode: off')

def test_pager_on_with_pager_env(shell):
    """Test that pager uses PAGER environment variable"""
    test = (
        ShellTest(shell)
            .statement('.pager on')
            .statement('.pager')
    )
    test.environment['PAGER'] = 'less'
    result = test.run()
    result.check_stdout('less')


def test_pager_on_with_duckdb_pager_env(shell):
    """Test that DUCKDB_PAGER takes precedence over PAGER"""
    test = (
        ShellTest(shell)
            .statement('.pager on')
            .statement('.pager')
    )
    test.environment['PAGER'] = 'less'
    test.environment['DUCKDB_PAGER'] = 'less -SR'
    result = test.run()
    result.check_stdout('less -SR')


def test_pager_duckdb_pager_priority(shell):
    """Test DUCKDB_PAGER shows in status even when pager is off"""
    test = (
        ShellTest(shell)
            .statement('.pager on')
            .statement('.pager')
    )
    test.environment['DUCKDB_PAGER'] = 'less -R'
    result = test.run()
    result.check_stdout('less -R')


def test_pager_custom_command(shell):
    """Test setting a custom pager command"""
    test = (
        ShellTest(shell)
            .statement(".pager 'cat'")
            .statement('.pager')
    )
    result = test.run()
    result.check_stdout('cat')


def test_pager_custom_command_with_args(shell):
    """Test setting a custom pager command with arguments"""
    test = (
        ShellTest(shell)
            .statement(".pager 'less -SR'")
            .statement('.pager')
    )
    result = test.run()
    result.check_stdout('less -SR')

def test_pager_with_query_output(shell):
    """Test that pager works with query output using cat"""
    test = (
        ShellTest(shell)
            .statement(".mode csv")
            .statement(".pager 'cat'")
            .statement('FROM range(10000)')
    )
    result = test.run()
    result.check_stdout('8888')

def test_pager_doesnt_affect_error_messages(shell):
    """Test that pager doesn't capture error messages"""
    test = (
        ShellTest(shell)
            .statement(".pager 'cat'")
            .statement(".mode csv")
            .statement('SELECT invalid_column FROM nonexistent_table')
    )
    result = test.run()
    result.check_stderr('Table')


def test_pager_preserves_nullvalue(shell):
    """Test that pager preserves null value rendering"""
    test = (
        ShellTest(shell)
            .statement('.nullvalue XYZ')
            .statement(".pager 'cat'")
            .statement('SELECT NULL FROM range(10000)')
    )
    result = test.run()
    result.check_stdout('XYZ')

def test_pager_multiple_queries(shell):
    """Test pager with multiple queries in sequence"""
    test = (
        ShellTest(shell)
            .statement(".pager 'cat'")
            .statement(".mode csv")
            .statement('SELECT 1 FROM range(1000)')
            .statement('SELECT 2 FROM range(1000)')
            .statement('SELECT 3 FROM range(1000)')
    )
    result = test.run()
    result.check_stdout('1')
    result.check_stdout('2')
    result.check_stdout('3')

def test_pager_small_data(shell):
    """Test pager with a small data set"""
    test = (
        ShellTest(shell)
            .statement(".pager 'unknown_cmd'")
            .statement(".mode csv")
            .statement('FROM range(10)')
    )
    result = test.run()
    result.check_stdout('9')

# fmt: on
