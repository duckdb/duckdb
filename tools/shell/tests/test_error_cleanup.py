import subprocess
from pathlib import Path

import pytest


def run_shell(shell, database, arguments, stdin=""):
    return subprocess.run(
        [shell, "-no-init", "-batch", *([str(database)] if database else []), *arguments],
        input=stdin,
        text=True,
        capture_output=True,
        timeout=30,
    )


def assert_checkpointed(database):
    # Check before reopening: recovery would otherwise hide the skipped destructor.
    assert database.is_file()
    assert not Path(str(database) + ".wal").exists()


@pytest.mark.parametrize("mode", ["-c", "-cmd", "-f", "-ui", "missing-file", "meta-command", "ascii-import"])
def test_command_error_cleans_up_database(shell, tmp_path, mode):
    database = tmp_path / "cleanup.duckdb"
    setup = "SET wal_autocheckpoint='1TB'; CREATE TABLE saved AS SELECT 42 AS value;"
    failure = "SELECT * FROM missing_cleanup_table;"
    expected_error = "missing_cleanup_table"
    arguments = ["-c", setup]
    if mode == "-f":
        script = tmp_path / "error.sql"
        script.write_text(failure + "\nSELECT 'SHOULD_NOT_RUN';\n")
        arguments += ["-bail", "-f", str(script)]
    elif mode == "-ui":
        arguments += ["-cmd", ".ui_command missing_cleanup_function()", "-ui"]
        expected_error = "missing_cleanup_function"
    elif mode == "missing-file":
        arguments += ["-f", str(tmp_path / "missing_cleanup_file.sql")]
        expected_error = "missing_cleanup_file.sql"
    elif mode == "ascii-import":
        arguments += ["-c", ".import -ascii unused.csv saved"]
        expected_error = "-ascii mode is no longer supported"
    elif mode == "meta-command":
        arguments += ["-c", ".missing_cleanup_command"]
        expected_error = "missing_cleanup_command"
    else:
        arguments += [mode, failure]
    arguments += ["-c", "SELECT 'SHOULD_NOT_RUN';"]
    result = run_shell(shell, database, arguments, "SELECT 'STDIN_SHOULD_NOT_RUN';\n")
    assert result.returncode == 1, result.stderr
    assert expected_error in result.stderr
    assert "SHOULD_NOT_RUN" not in result.stdout
    assert_checkpointed(database)

    reopened = run_shell(shell, database, ["-csv", "-noheader", "-c", "SELECT * FROM saved;"])
    assert reopened.returncode == 0, reopened.stderr
    assert reopened.stdout.strip() == "42"


@pytest.mark.parametrize("mode", ["-c", "-cmd", "-f"])
def test_successful_command_cleans_up_database(shell, tmp_path, mode):
    database = tmp_path / "success.duckdb"
    sql = "CREATE TABLE saved AS SELECT 42 AS value; SELECT 'SUCCESS';"
    if mode == "-f":
        script = tmp_path / "success.sql"
        script.write_text(sql)
        arguments = [mode, str(script)]
    else:
        arguments = [mode, sql]
    result = run_shell(shell, database, arguments)
    assert result.returncode == 0, result.stderr
    assert "SUCCESS" in result.stdout
    assert_checkpointed(database)


@pytest.mark.parametrize("mode", ["-c", "-cmd", "-f"])
def test_no_bail_retains_command_behavior(shell, tmp_path, mode):
    database = tmp_path / "no_bail.duckdb"
    arguments = ["-cmd", ".bail off", "-cmd", "CREATE TABLE saved AS SELECT 42 AS value;"]
    failure = "SELECT * FROM missing_cleanup_table;"
    if mode == "-f":
        script = tmp_path / "no_bail.sql"
        script.write_text(failure + "\nSELECT 'FILE_CONTINUED';\n")
        arguments += [mode, str(script)]
    else:
        arguments += [mode, failure]
    arguments += ["-cmd", "SELECT 'NEXT_COMMAND';"]
    result = run_shell(shell, database, arguments, "SELECT 'STDIN_CONTINUED';\n")
    # -f still reports errors after processing the file; -c/-cmd honor .bail off.
    assert result.returncode == (1 if mode == "-f" else 0), result.stderr
    assert "missing_cleanup_table" in result.stderr
    if mode == "-f":
        assert "FILE_CONTINUED" in result.stdout
        assert "NEXT_COMMAND" not in result.stdout
    else:
        assert "NEXT_COMMAND" in result.stdout
    assert ("STDIN_CONTINUED" in result.stdout) == (mode == "-cmd")
    assert_checkpointed(database)


@pytest.mark.parametrize("option", ["-help", "-version"])
def test_informational_exit_stays_successful(shell, tmp_path, option):
    result = run_shell(shell, tmp_path / "unused.duckdb", [option])
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("mode", ["-serve", "-connect", "parameter", "-manual"])
def test_configured_command_error_cleans_up_database(shell, tmp_path, mode):
    database = tmp_path / "configured.duckdb"
    # -connect rejects a positional database, so open it through a prior callback.
    arguments = ["-cmd", f'.open "{database.as_posix()}"', "-cmd", "CREATE TABLE saved AS SELECT 42 AS value;"]
    expected_error = "missing_cleanup_table"
    if mode == "-manual":
        arguments += ["-manual", "too.many.name.components"]
        expected_error = "not a valid function name"
    elif mode == "parameter":
        arguments += ["-cmd", ".serve_command .print {missing_cleanup_parameter}", "-serve"]
        expected_error = "missing_cleanup_parameter"
    else:
        command = ".serve_command" if mode == "-serve" else ".connect_command"
        arguments += ["-cmd", command + " SELECT * FROM missing_cleanup_table;", mode]
    arguments += ["-c", "SELECT 'SHOULD_NOT_RUN';"]
    result = run_shell(shell, None, arguments)
    assert result.returncode == 1, result.stderr
    assert expected_error in result.stderr
    assert "SHOULD_NOT_RUN" not in result.stdout
    assert_checkpointed(database)


@pytest.mark.parametrize("mode", ["-format", "-format-file"])
def test_format_failure_stops_following_commands(shell, tmp_path, mode):
    database = tmp_path / "format.duckdb"
    # Force a deterministic formatter failure, independent of installed extensions.
    setup = "CREATE TABLE saved AS SELECT 42 AS value; CREATE MACRO duckdb_format_sql(s) AS error('format_cleanup_failure');"
    arguments = ["-c", setup, mode]
    if mode == "-format-file":
        script = tmp_path / "format.sql"
        script.write_text("select 1")
        arguments.append(str(script))
    arguments += ["-c", "SELECT 'SHOULD_NOT_RUN';"]
    result = run_shell(shell, database, arguments, "select 1")
    assert result.returncode == 1, result.stderr
    assert "format_cleanup_failure" in result.stderr
    assert "SHOULD_NOT_RUN" not in result.stdout
    assert_checkpointed(database)
