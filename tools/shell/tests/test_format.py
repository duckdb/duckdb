# fmt: off

import pytest
from conftest import ShellTest


def sql_file(tmp_path, name, content):
    """Write content to a temp SQL file and return its path."""
    p = tmp_path / name
    p.write_text(content)
    return p


def run_format(shell, path):
    """Run --format-file on path and return (TestResult, stdout_contents)."""
    result = ShellTest(shell).add_argument("-format-file", str(path)).run()
    return result, result.stdout


# All format tests require the autocomplete extension (duckdb_format_sql lives there).
pytestmark = pytest.mark.usefixtures("autocomplete_extension")


def test_format_basic_select(shell, tmp_path):
    """Keywords are uppercased; clause keywords each start a new line."""
    p = sql_file(tmp_path, "q.sql", "select a,b,c from t where x=1 and y=2 group by a,b order by a desc limit 10")
    result, contents = run_format(shell, p)
    assert result.status_code == 0
    assert "SELECT a, b, c" in contents
    assert "FROM t" in contents
    assert "WHERE x = 1 AND y = 2" in contents
    assert "GROUP BY a, b" in contents
    assert "ORDER BY a DESC" in contents
    assert "LIMIT 10" in contents

def test_format_does_not_read_stdin(shell, tmp_path):
    """--format-file must not block waiting for stdin; formatted SQL goes to stdout."""
    p = sql_file(tmp_path, "q.sql", "select 1")
    result, contents = run_format(shell, p)
    assert result.status_code == 0
    assert "SELECT 1" in contents


def test_format_missing_file(shell, tmp_path):
    """A clear error is printed and the exit code is non-zero for missing files."""
    missing = tmp_path / "does_not_exist.sql"
    result = ShellTest(shell).add_argument("-format-file", str(missing)).run()
    result.check_stderr("cannot open")
    assert result.status_code != 0


def test_format_empty_file(shell, tmp_path):
    """An empty file is handled cleanly (no crash, exit 0)."""
    p = sql_file(tmp_path, "empty.sql", "")
    result, _ = run_format(shell, p)
    assert result.status_code == 0


# ---------------------------------------------------------------------------
# -format (stdin -> stdout) tests
# ---------------------------------------------------------------------------

def run_format_stdin(shell, sql_input):
    """Pipe sql_input through 'duckdb -format' and return a TestResult."""
    result = ShellTest(shell).add_argument("-format").input_file(None).run_raw(sql_input)
    return result


def test_format_stdin_basic(shell):
    """Basic SELECT via stdin is formatted and printed to stdout."""
    result = run_format_stdin(shell, "select a, b from t where x = 1")
    assert result.status_code == 0
    assert "SELECT a, b" in result.stdout
    assert "FROM t" in result.stdout
    assert "WHERE x = 1" in result.stdout

def test_format_stdin_multi_statement(shell):
    """Multiple statements are formatted correctly via stdin."""
    result = run_format_stdin(shell, "select a from t; select b from u")
    assert result.status_code == 0
    assert "SELECT a\nFROM t;\nSELECT b\nFROM u" in result.stdout.replace("\r\n", "\n")

def test_format_stdin_empty(shell):
    """Empty stdin exits cleanly."""
    result = run_format_stdin(shell, "")
    assert result.status_code == 0

# fmt: on
