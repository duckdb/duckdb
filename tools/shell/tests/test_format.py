# fmt: off

import pytest
from conftest import ShellTest


def sql_file(tmp_path, name, content):
    """Write content to a temp SQL file and return its path."""
    p = tmp_path / name
    p.write_text(content)
    return p


def run_format(shell, path):
    """Run --format-file on path and return (TestResult, file_contents)."""
    result = ShellTest(shell).add_argument("-format-file", str(path)).run()
    contents = path.read_text() if path.exists() else ""
    return result, contents


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


def test_format_multi_statement(shell, tmp_path):
    """Multiple statements are separated by semicolons in the output."""
    p = sql_file(tmp_path, "q.sql", "select a from t; select b from u")
    result, contents = run_format(shell, p)
    assert result.status_code == 0
    assert "SELECT a\nFROM t;\nSELECT b\nFROM u" in contents


def test_format_join(shell, tmp_path):
    """JOIN clauses are placed on their own lines."""
    p = sql_file(tmp_path, "q.sql",
        "select a from t1 inner join t2 on t1.id = t2.id where t1.active = true")
    result, contents = run_format(shell, p)
    assert result.status_code == 0
    assert "INNER JOIN t2 ON t1.id = t2.id" in contents
    assert "WHERE t1.active = TRUE" in contents


def test_format_cte(shell, tmp_path):
    """CTE bodies are indented; the outer query follows normally."""
    p = sql_file(tmp_path, "q.sql",
        "WITH cte AS (SELECT id FROM users WHERE active = 1) "
        "SELECT id FROM cte GROUP BY id HAVING count(*) > 5")
    result, contents = run_format(shell, p)
    assert result.status_code == 0
    expected = (
        "WITH cte AS (\n"
        "        SELECT id\n"
        "        FROM users\n"
        "        WHERE active = 1\n"
        "    )\n"
        "SELECT id\n"
        "FROM cte\n"
        "GROUP BY id\n"
        "HAVING count(*) > 5"
    )
    assert expected in contents


def test_format_subquery_in_where(shell, tmp_path):
    """Subqueries inside WHERE IN are indented one level deeper."""
    p = sql_file(tmp_path, "q.sql",
        "select id from orders where customer_id in "
        "(select id from customers where active = 1) and amount > 100")
    result, contents = run_format(shell, p)
    assert result.status_code == 0
    assert "    customer_id IN (" in contents
    assert "        SELECT id" in contents
    assert "        FROM customers" in contents
    assert "    AND amount > 100" in contents


def test_format_create_table_single_column(shell, tmp_path):
    """Single-column CREATE TABLE stays on one line."""
    p = sql_file(tmp_path, "q.sql", "CREATE TABLE t (id INTEGER PRIMARY KEY)")
    result, contents = run_format(shell, p)
    assert result.status_code == 0
    assert "CREATE TABLE t(id INTEGER PRIMARY KEY)" in contents


def test_format_create_table_multi_column(shell, tmp_path):
    """Multi-column CREATE TABLE gets one column per line."""
    p = sql_file(tmp_path, "q.sql",
        "CREATE TABLE orders ("
        "id INTEGER PRIMARY KEY, "
        "customer_id INTEGER NOT NULL REFERENCES customers(id), "
        "amount DECIMAL(10,2) DEFAULT 0, "
        "status VARCHAR(20)"
        ")")
    result, contents = run_format(shell, p)
    assert result.status_code == 0
    expected = (
        "CREATE TABLE orders(\n"
        "    id INTEGER PRIMARY KEY,\n"
        "    customer_id INTEGER NOT NULL REFERENCES customers(id),\n"
        "    amount DECIMAL(10, 2) DEFAULT 0,\n"
        "    status VARCHAR(20)\n"
        ")"
    )
    assert expected in contents


def test_format_create_table_constraints(shell, tmp_path):
    """Table-level PRIMARY KEY and FOREIGN KEY constraints each get their own line."""
    p = sql_file(tmp_path, "q.sql",
        "CREATE TABLE t (a INTEGER, b INTEGER, "
        "PRIMARY KEY (a, b), FOREIGN KEY (b) REFERENCES other(id))")
    result, contents = run_format(shell, p)
    assert result.status_code == 0
    expected = (
        "CREATE TABLE t(\n"
        "    a INTEGER,\n"
        "    b INTEGER,\n"
        "    PRIMARY KEY (a, b),\n"
        "    FOREIGN KEY (b) REFERENCES other(id)\n"
        ")"
    )
    assert expected in contents


def test_format_create_view(shell, tmp_path):
    """CREATE VIEW is formatted with the SELECT body on following lines."""
    p = sql_file(tmp_path, "q.sql",
        "CREATE VIEW active_users AS SELECT id, name FROM users WHERE active = true")
    result, contents = run_format(shell, p)
    assert result.status_code == 0
    assert "CREATE VIEW active_users AS\nSELECT id, name\nFROM users\nWHERE active = TRUE" in contents


def test_format_overwrites_file(shell, tmp_path):
    """The file is overwritten in-place with the formatted SQL."""
    p = sql_file(tmp_path, "q.sql", "select 1")
    original_path = str(p)
    result, contents = run_format(shell, p)
    assert result.status_code == 0
    # The file at the same path now contains formatted SQL.
    assert "SELECT 1" in contents
    # The original unformatted content is gone.
    assert "select 1" not in contents


def test_format_does_not_read_stdin(shell, tmp_path):
    """--format-file must not block waiting for stdin; it exits immediately."""
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
    assert "SELECT a\nFROM t;\nSELECT b\nFROM u" in result.stdout


def test_format_stdin_join(shell):
    """JOIN clauses are formatted when piped via stdin."""
    result = run_format_stdin(shell, "select a from t1 inner join t2 on t1.id = t2.id where t1.active = true")
    assert result.status_code == 0
    assert "INNER JOIN t2 ON t1.id = t2.id" in result.stdout
    assert "WHERE t1.active = TRUE" in result.stdout


def test_format_stdin_cte(shell):
    """CTE formatting works via stdin."""
    sql = ("WITH cte AS (SELECT id FROM users WHERE active = 1) "
           "SELECT id FROM cte GROUP BY id HAVING count(*) > 5")
    result = run_format_stdin(shell, sql)
    assert result.status_code == 0
    assert "WITH cte AS (" in result.stdout
    assert "SELECT id" in result.stdout
    assert "FROM cte" in result.stdout
    assert "GROUP BY id" in result.stdout
    assert "HAVING count(*) > 5" in result.stdout


def test_format_stdin_empty(shell):
    """Empty stdin exits cleanly."""
    result = run_format_stdin(shell, "")
    assert result.status_code == 0


def test_format_stdin_does_not_modify_files(shell, tmp_path):
    """The -format flag only writes to stdout, not to any file."""
    result = run_format_stdin(shell, "select 1")
    assert result.status_code == 0
    assert "SELECT 1" in result.stdout


def test_format_subquery_select_list_multiline(shell):
    """SELECT columns inside a JOIN subquery each get their own line when the list is long."""
    sql = (
        "SELECT id FROM t "
        "LEFT JOIN ("
        "SELECT avg(a.address_valuation) AS avg_val, sum(a.address_zone) AS sum_zone, count(a.address_zone) AS cnt_zone "
        "FROM s GROUP BY s.id"
        ") sub ON t.id = sub.id"
    )
    result = run_format_stdin(shell, sql)
    assert result.status_code == 0
    # Each column in the subquery's SELECT list should be on its own line
    assert "avg(a.address_valuation) AS avg_val,\n" in result.stdout
    assert "sum(a.address_zone) AS sum_zone,\n" in result.stdout
    assert "count(a.address_zone) AS cnt_zone\n" in result.stdout
    # Columns must NOT all run together on one line
    assert "avg_val, sum(" not in result.stdout

# fmt: on
