import re

import pytest

from conftest import ShellTest


def test_trace(shell, tmp_path):
    temp_dir = tmp_path / 'http_logging_dir'
    temp_dir.mkdir()
    temp_file = temp_dir / 'myfile'

    test = (
        ShellTest(shell)
        .statement("CALL enable_logging('FileSystem', level = 'trace', sink = 'shell_log_sink');")
        .statement(f"copy (select 1 as a) to '{temp_file.as_posix()}'")
    )
    result = test.run()
    result.check_stdout("TRACE:")


# FIXME: DUCKDB_LOG_DEBUG hasn't been used anywhere yet.
# def test_debug(shell):
#     test = (
#         ShellTest(shell)
#         .statement("CALL enable_logging(level = 'debug', sink = 'shell_log_sink');")
#         .statement("SELECT 42;")
#     )
#
#     result = test.run()
#     result.check_stdout("DEBUG:")


def test_info(shell):
    probe = ShellTest(shell).statement("LOAD HTTP;").run()
    if probe.status_code != 0:
        pytest.skip("DuckDB HTTP extension not installed/available")

    test = (
        ShellTest(shell)
        .statement("CALL enable_logging(level = 'info', sink = 'shell_log_sink');")
        .statement("LOAD HTTP;")
    )

    result = test.run()
    result.check_stdout("INFO:")
    result.check_stdout("LOAD HTTP")


@pytest.mark.skip(reason="Needs a new warning")
def test_warning(shell):
    test = ShellTest(shell).statement("SELECT list_transform([1], x -> x);")

    result = test.run()
    result.check_stdout("WARNING:")
    result.check_stdout("Deprecated lambda arrow (->) detected.")
    result.check_stdout("[1]")
    result.check_stderr(None)


@pytest.mark.skip(reason="Needs a new warning")
def test_warning_as_error(shell):
    test = ShellTest(shell)
    test.statement("SET warnings_as_errors = true")
    test.statement("SELECT list_transform([1], x -> x + 1)")
    result = test.run()
    result.check_stderr("Deprecated lambda arrow (->) detected.")
    assert result.status_code == 1


# Make sure that the same warning is not printed multiple times
@pytest.mark.skip(reason="Needs a new warning")
def test_multiple_warnings(shell):
    test = ShellTest(shell).statement(
        "select list_filter([1,2,3], x -> x > 2) l1, list_filter([1,2,3], x -> x > 2) l2, list_filter([1,2,3], x -> x > 2) l3;"
    )

    result = test.run()
    assert result.stdout.count("WARNING:") == 1
    result.check_stdout("Deprecated lambda arrow (->) detected.")
    result.check_stdout("│ [3]     │ [3]     │ [3]     │")


def standalone_lines(result):
    # the result boxes also show the SQL of the column names, so only lines of their own are printed log entries.
    # The warnings are printed in colour, without the colour codes the line is the message
    return [re.sub(r'\x1b\[[0-9;]*m', '', line).strip() for line in result.stdout.splitlines()]


def test_changing_the_default_sink_stops_printing_warnings(shell, tmp_path):
    # the shell's sink is the default log sink in the CLI, another default log sink replaces it
    log_dir = tmp_path / 'log_dir'
    test = (
        ShellTest(shell)
        .statement(
            f"CALL enable_logging(level = 'warning', sink = 'file', sink_config = {{'path': '{log_dir.as_posix()}'}});"
        )
        .statement("SELECT write_log('not printed', level := 'WARNING') AS x;")
        .statement("CALL enable_log_sink('shell_log_sink', level := 'warning');")
        .statement("SELECT write_log('printed again', level := 'WARNING') AS x;")
    )

    result = test.run()
    result.check_stdout("The logging settings have been changed")
    result.check_stdout(
        "To keep printing warnings to the console, also run CALL enable_log_sink('shell_log_sink', level := 'warning')."
    )
    lines = standalone_lines(result)
    assert "not printed" not in lines
    assert "printed again" in lines


def test_deprecated_shell_log_storage(shell):
    test = ShellTest(shell).statement("CALL enable_logging(level = 'info', sink = 'shell_log_storage');")

    result = test.run()
    result.check_stdout("WARNING:")
    result.check_stdout("The log sink 'shell_log_storage' has been renamed to 'shell_log_sink'")
    # the deprecated name still selects the shell's sink
    result.check_not_exist("The logging settings have been changed")


def test_keeping_shell_log_sink_does_not_warn(shell):
    test = (
        ShellTest(shell)
        .statement("CALL enable_logging(level = 'warning');")
        .statement("CALL enable_logging(level = 'info', sink = 'shell_log_sink');")
    )

    result = test.run()
    result.check_not_exist("The logging settings have been changed")


@pytest.mark.parametrize(
    "statement, example",
    [
        (
            "CALL enable_logging(level = 'error');",
            "CALL enable_log_sink('my_logs', type := 'memory', level := 'error').",
        ),
        (
            "CALL enable_logging('QueryLog');",
            "CALL enable_log_sink('my_logs', type := 'memory', log_types := ['QueryLog']).",
        ),
    ],
)
def test_dropping_warnings_from_shell_log_sink_warns(shell, statement, example):
    test = ShellTest(shell).statement(statement)

    result = test.run()
    result.check_stdout("The logging settings have been changed")
    result.check_stdout("The default log sink prints the warnings to the console")
    result.check_stdout(example)


def test_named_shell_log_sink_keeps_printing_warnings(shell):
    # a shell sink of its own has its own level, enable_logging changes the default log sink only
    test = (
        ShellTest(shell)
        .statement("CALL enable_logging(sink = 'memory');")
        .statement("CALL enable_log_sink('shell_log_sink', level := 'warning');")
        .statement("CALL enable_logging(level = 'error');")
        .statement("SELECT write_log('still printed', level := 'WARNING') AS x;")
    )

    result = test.run()
    # only leaving the shell's sink as the default log sink warns
    assert result.stdout.count("The logging settings have been changed") == 1
    assert "still printed" in standalone_lines(result)


def test_enable_shell_log_sink(shell):
    # by default the CLI prints the warnings with its default log sink
    test = (
        ShellTest(shell)
        .statement("SELECT name || ':' || type AS sinks FROM duckdb_log_sinks();")
        .statement("SELECT write_log('printed', level := 'WARNING') AS x;")
    )
    result = test.run()
    result.check_stdout("default:shell_log_sink")
    assert "printed" in standalone_lines(result)

    # without logging, the warnings are no longer printed
    test = (
        ShellTest(shell)
        .statement("CALL disable_logging();")
        .statement("SELECT write_log('not printed', level := 'WARNING') AS x;")
    )
    result = test.run()
    assert "not printed" not in standalone_lines(result)

    # enabling logging prints them again
    test = (
        ShellTest(shell)
        .statement("CALL disable_logging();")
        .statement("CALL enable_logging();")
        .statement("SELECT write_log('printed again', level := 'WARNING') AS x;")
    )
    result = test.run()
    assert "printed again" in standalone_lines(result)
