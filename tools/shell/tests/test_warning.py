import pytest

from conftest import ShellTest


def test_trace(shell, tmp_path):
    temp_dir = tmp_path / 'http_logging_dir'
    temp_dir.mkdir()
    temp_file = temp_dir / 'myfile'

    test = (
        ShellTest(shell)
        .statement("CALL enable_logging('FileSystem', level = 'trace', storage = 'shell_log_storage');")
        .statement(f"copy (select 1 as a) to 'temp_file'")
    )
    result = test.run()
    result.check_stdout("TRACE:")


# FIXME: DUCKDB_LOG_DEBUG hasn't been used anywhere yet.
# def test_debug(shell):
#     test = (
#         ShellTest(shell)
#         .statement("CALL enable_logging(level = 'debug', storage = 'shell_log_storage');")
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
        .statement("CALL enable_logging(level = 'info', storage = 'shell_log_storage');")
        .statement("LOAD HTTP;")
    )

    result = test.run()
    result.check_stdout("INFO:")
    result.check_stdout("LOAD HTTP")


def test_warning(shell):
    test = ShellTest(shell).statement("SELECT list_transform([1], x -> x);")

    result = test.run()
    result.check_stdout("WARNING:")
    result.check_stdout("Deprecated lambda arrow (->) detected.")
    result.check_stdout("[1]")


def test_changing_logging_settings(shell, tmp_path):
    test = ShellTest(shell).statement("CALL enable_logging(storage = 'file', storage_config = {'path': 'hello'});")

    result = test.run()
    print(result.stderr)
    result.check_stdout("WARNING:")
    result.check_stdout("The logging settings have been changed")
