# fmt: off

import pytest
from conftest import ShellTest

def test_pager_status_default(shell):
    """Test that pager status shows 'off' by default"""
    test = (
        ShellTest(shell)
            .statement('.pager')
    )
    result = test.run()
    result.check_stdout('Pager mode: off')


def test_pager_help(shell):
    """Test that .help shows pager documentation"""
    test = (
        ShellTest(shell)
            .statement('.help pager')
    )
    result = test.run()
    result.check_stdout('.pager ?on|off|<cmd>?')
    result.check_stdout('Control pager usage for output')
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


def test_pager_on_without_env(shell):
    """Test that enabling pager without environment variable shows warning"""
    test = (
        ShellTest(shell)
            .statement('.pager on')
    )
    result = test.run()
    result.check_stderr('Warning: No pager configured')
    result.check_stderr('Set DUCKDB_PAGER or PAGER environment variable')


def test_pager_on_with_pager_env(shell):
    """Test that pager uses PAGER environment variable"""
    test = (
        ShellTest(shell)
            .statement('.pager on')
            .statement('.pager')
    )
    test.environment['PAGER'] = 'less'
    result = test.run()
    result.check_stdout('Pager mode: on')
    result.check_stdout('Pager command: less')


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
    result.check_stdout('Pager mode: on')
    result.check_stdout('Pager command: less -SR')


def test_pager_duckdb_pager_priority(shell):
    """Test DUCKDB_PAGER shows in status even when pager is off"""
    test = (
        ShellTest(shell)
            .statement('.pager')
    )
    test.environment['DUCKDB_PAGER'] = 'less -R'
    result = test.run()
    result.check_stdout('Pager mode: off')
    result.check_stdout('Pager command: less -R')


def test_pager_custom_command(shell):
    """Test setting a custom pager command"""
    test = (
        ShellTest(shell)
            .statement(".pager 'cat'")
            .statement('.pager')
    )
    result = test.run()
    result.check_stdout('Pager mode: off')
    result.check_stdout('Pager command: cat')


def test_pager_custom_command_with_args(shell):
    """Test setting a custom pager command with arguments"""
    test = (
        ShellTest(shell)
            .statement(".pager 'less -SR'")
            .statement('.pager')
    )
    result = test.run()
    result.check_stdout('Pager mode: off')
    result.check_stdout('Pager command: less -SR')


def test_pager_toggle_on_off(shell):
    """Test toggling pager on and off"""
    test = (
        ShellTest(shell)
            .statement(".pager 'cat'")
            .statement('.pager')
            .statement('.pager on')
            .statement('.pager')
            .statement('.pager off')
            .statement('.pager')
    )
    result = test.run()
    result.check_stdout('Pager mode: off')
    result.check_stdout('Pager mode: on')
    result.check_stdout('Pager mode: off')


def test_pager_with_query_output(shell):
    """Test that pager works with query output using cat"""
    test = (
        ShellTest(shell)
            .statement(".pager 'cat'")
            .statement('SELECT 42')
    )
    result = test.run()
    result.check_stdout('42')


def test_pager_with_large_output(shell):
    """Test pager with large query result using cat"""
    test = (
        ShellTest(shell)
            .statement(".pager 'cat'")
            .statement('SELECT * FROM range(100)')
    )
    result = test.run()
    result.check_stdout('99')


def test_pager_with_csv_mode(shell):
    """Test pager with CSV output mode"""
    test = (
        ShellTest(shell)
            .statement('.mode csv')
            .statement(".pager 'cat'")
            .statement('SELECT 1, 2, 3')
    )
    result = test.run()
    result.check_stdout('1,2,3')


def test_pager_with_duckbox_mode(shell):
    """Test pager with duckbox output mode"""
    test = (
        ShellTest(shell)
            .statement('.mode duckbox')
            .statement(".pager 'cat'")
            .statement('SELECT 42')
    )
    result = test.run()
    result.check_stdout('42')


def test_pager_doesnt_affect_error_messages(shell):
    """Test that pager doesn't capture error messages"""
    test = (
        ShellTest(shell)
            .statement(".pager 'cat'")
            .statement('SELECT invalid_column FROM nonexistent_table')
    )
    result = test.run()
    result.check_stderr('Table')


def test_pager_with_output_file(shell, random_filepath):
    """Test that pager is not used when output is redirected to file"""
    test = (
        ShellTest(shell)
            .statement(".pager 'cat'")
            .statement(f'.output {random_filepath.as_posix()}')
            .statement('SELECT 84')
            .statement('.output')
            .statement('SELECT 42')
    )
    result = test.run()
    # File should contain 84, stdout should contain 42
    file_content = open(random_filepath, 'r').read()
    assert '84' in file_content
    result.check_stdout('42')


def test_pager_preserves_nullvalue(shell):
    """Test that pager preserves null value rendering"""
    test = (
        ShellTest(shell)
            .statement('.nullvalue NULL')
            .statement(".pager 'cat'")
            .statement('SELECT NULL')
    )
    result = test.run()
    result.check_stdout('NULL')


def test_pager_with_headers(shell):
    """Test that pager includes headers"""
    test = (
        ShellTest(shell)
            .statement('.headers on')
            .statement(".pager 'cat'")
            .statement('SELECT 42 AS answer')
    )
    result = test.run()
    result.check_stdout('answer')
    result.check_stdout('42')


def test_pager_command_trimming(shell):
    """Test that pager command is trimmed of whitespace"""
    test = (
        ShellTest(shell)
            .statement(".pager '  cat  '")
            .statement('.pager')
    )
    result = test.run()
    result.check_stdout('Pager mode: off')


def test_pager_empty_string(shell):
    """Test that empty string disables pager"""
    test = (
        ShellTest(shell)
            .statement(".pager 'cat'")
            .statement(".pager ''")
            .statement('.pager')
    )
    result = test.run()
    result.check_stdout('Pager mode: off')


def test_pager_multiple_queries(shell):
    """Test pager with multiple queries in sequence"""
    test = (
        ShellTest(shell)
            .statement(".pager 'cat'")
            .statement('SELECT 1')
            .statement('SELECT 2')
            .statement('SELECT 3')
    )
    result = test.run()
    result.check_stdout('1')
    result.check_stdout('2')
    result.check_stdout('3')


def test_pager_with_columnar_mode(shell):
    """Test pager with columnar output mode"""
    test = (
        ShellTest(shell)
            .statement('.col')
            .statement(".pager 'cat'")
            .statement('SELECT * FROM range(5)')
    )
    result = test.run()
    result.check_stdout('Row')


def test_pager_with_mode_changes(shell):
    """Test pager persists across mode changes"""
    test = (
        ShellTest(shell)
            .statement(".pager 'cat'")
            .statement('.mode csv')
            .statement('SELECT 1, 2')
            .statement('.mode duckbox')
            .statement('SELECT 3, 4')
    )
    result = test.run()
    result.check_stdout('1,2')
    result.check_stdout('3')
    result.check_stdout('4')


def test_pager_with_timer(shell):
    """Test that timer output is not paged"""
    test = (
        ShellTest(shell)
            .statement('.timer on')
            .statement(".pager 'cat'")
            .statement('SELECT 42')
    )
    result = test.run()
    result.check_stdout('42')
    result.check_stdout('Run Time')


def test_pager_invalid_command_error(shell):
    """Test that invalid pager command shows error"""
    test = (
        ShellTest(shell)
            .statement(".pager '/this/command/does/not/exist'")
            .statement('SELECT 42')
    )
    result = test.run()
    # Invalid pager produces shell error (sh: not found) but still displays output
    result.check_stdout('42')


def test_pager_with_show_command(shell):
    """Test pager status appears in .show output"""
    test = (
        ShellTest(shell)
            .statement(".pager 'cat'")
            .statement('.pager on')
            .statement('.show')
    )
    result = test.run()
    result.check_stdout('pager: cat (on)')


def test_pager_reset_to_default(shell):
    """Test that custom pager command persists after toggling off/on"""
    test = (
        ShellTest(shell)
            .statement(".pager 'cat'")
            .statement('.pager on')
            .statement('.pager off')
            .statement('.pager on')
            .statement('.pager')
    )
    test.environment['PAGER'] = 'less'
    result = test.run()
    # Custom pager command persists - it doesn't reset to env var
    result.check_stdout('Pager mode: on')
    result.check_stdout('Pager command: cat')


def test_pager_status_with_no_command_configured(shell):
    """Test status when pager is off and no command configured"""
    test = (
        ShellTest(shell)
            .statement('.pager')
    )
    # No environment variables set
    result = test.run()
    result.check_stdout('Pager mode: off')


def test_pager_multiple_statements_single_call(shell):
    """Test pager handles multiple statements in one run"""
    test = (
        ShellTest(shell)
            .statement(".pager 'cat'")
            .statement('CREATE TABLE test (i INTEGER)')
            .statement('INSERT INTO test VALUES (1), (2), (3)')
            .statement('SELECT * FROM test')
    )
    result = test.run()
    result.check_stdout('1')
    result.check_stdout('2')
    result.check_stdout('3')


def test_pager_with_transaction(shell):
    """Test pager works within a transaction"""
    test = (
        ShellTest(shell)
            .statement('BEGIN TRANSACTION')
            .statement(".pager 'cat'")
            .statement('SELECT 42')
            .statement('COMMIT')
    )
    result = test.run()
    result.check_stdout('42')


def test_pager_config_persistence(shell):
    """Test that pager configuration persists across queries"""
    test = (
        ShellTest(shell)
            .statement(".pager 'cat'")
            .statement('.pager on')
            .statement('SELECT 1')
            .statement('.pager')
            .statement('SELECT 2')
            .statement('.pager')
    )
    result = test.run()
    result.check_stdout('Pager mode: on')
    result.check_stdout('1')
    result.check_stdout('2')


# fmt: on
