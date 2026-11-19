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


def test_changing_the_default_sink_keeps_printing_warnings(shell, tmp_path):
    # the shell's sink is enabled by name in the CLI, so changing the default log sink does not drop it
    log_dir = tmp_path / 'log_dir'
    test = (
        ShellTest(shell)
        .statement(
            f"CALL enable_logging(level = 'warning', sink = 'file', sink_config = {{'path': '{log_dir.as_posix()}'}});"
        )
        .statement("SELECT write_log('still printed', level := 'WARNING') AS x;")
    )

    result = test.run()
    result.check_not_exist("The logging settings have been changed")
    result.check_stdout("still printed")


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
    "statement",
    ["CALL enable_logging(level = 'error');", "CALL enable_logging('QueryLog');"],
)
def test_dropping_warnings_from_shell_log_sink_warns(shell, statement):
    test = ShellTest(shell).statement(statement)

    result = test.run()
    result.check_stdout("The logging settings have been changed")
    result.check_stdout("The log level and the log types apply to every log sink")


def test_dropping_warnings_from_named_shell_log_sink_warns(shell):
    test = (
        ShellTest(shell)
        .statement("SET logging_sink = 'memory';")
        .statement("CALL enable_log_sink('shell_log_sink');")
        .statement("CALL enable_logging(level = 'error');")
    )

    result = test.run()
    result.check_stdout("The logging settings have been changed")


def test_switching_default_sink_with_named_shell_log_sink_does_not_warn(shell):
    # a shell sink enabled by name keeps printing warnings when the default log sink changes
    test = (
        ShellTest(shell)
        .statement("SET logging_sink = 'memory';")
        .statement("CALL enable_log_sink('shell_log_sink');")
        .statement("CALL enable_logging(sink = 'stdout');")
    )

    result = test.run()
    result.check_not_exist("The logging settings have been changed")


def test_enable_shell_log_sink(shell):
    # by default the CLI prints the warnings, and keeps them in the default memory sink
    test = (
        ShellTest(shell)
        .statement("SET logging_storage = 'memory';")
        .statement(
            "SELECT 'stored-' || count(*) AS stored FROM duckdb_logs WHERE message LIKE 'the logging_storage setting is deprecated%';"
        )
    )
    result = test.run()
    result.check_stdout("the logging_storage setting is deprecated")
    result.check_stdout("stored-1")

    # without the shell's sink, the warnings are no longer printed
    test = (
        ShellTest(shell)
        .statement("CALL disable_log_sink('shell_log_sink');")
        .statement("SET logging_storage = 'memory';")
    )
    result = test.run()
    assert "the logging_storage setting is deprecated" not in result.stdout

    # enabling the shell's sink prints them again
    test = (
        ShellTest(shell)
        .statement("CALL disable_log_sink('shell_log_sink');")
        .statement("CALL enable_log_sink('shell_log_sink');")
        .statement("SET logging_storage = 'memory';")
    )
    result = test.run()
    result.check_stdout("the logging_storage setting is deprecated")

    # the warning about leaving the shell's sink as the default log sink says how to keep it
    test = (
        ShellTest(shell)
        .statement("CALL enable_logging(level = 'warning', sink = 'shell_log_sink');")
        .statement("CALL enable_logging(level = 'warning', sink = 'memory');")
    )
    result = test.run()
    result.check_stdout("To keep printing warnings to the console, also run CALL enable_log_sink('shell_log_sink').")
