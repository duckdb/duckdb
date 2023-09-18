# fmt: off

import pytest
import subprocess
import sys
from typing import List
from conftest import ShellTest, assert_expected_err, assert_expected_res
import os


def test_basic(shell):
    test = ShellTest(shell).statement("select 'asdf' as a")
    out, err, status = test.run()
    assert_expected_res(out, "asdf", status, err)


def test_range(shell):
    test = ShellTest(shell).statement("select * from range(10000)")
    out, err, status = test.run()
    assert_expected_res(out, "9999", status, err)


@pytest.mark.parametrize('generated_file', ["col_1,col_2\n1,2\n10,20"], indirect=True)
def test_import(shell, generated_file):
    test = (
        ShellTest(shell)
        .statement(".mode csv")
        .statement(f'.import "{generated_file}" test_table')
        .statement("select * FROM test_table")
    )

    out, err, status = test.run()
    assert_expected_res(out, "col_1,col_2\n1,2\n10,20", status, err)


@pytest.mark.parametrize('generated_file', ["42\n84"], indirect=True)
def test_import_sum(shell, generated_file):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE a (i INTEGER)")
        .statement(f'.import "{generated_file}" a')
        .statement("SELECT SUM(i) FROM a")
    )

    out, err, status = test.run()
    assert_expected_res(out, "126", status, err)


def test_pragma(shell):
    test = (
        ShellTest(shell)
        .statement(".mode csv")
        .statement(".headers off")
        .statement(".sep |")
        .statement("CREATE TABLE t0(c0 INT);")
        .statement("PRAGMA table_info('t0');")
    )

    out, err, status = test.run()
    assert_expected_res(out, "0|c0|INTEGER|false||false", status, err)


def test_system_functions(shell):
    test = ShellTest(shell).statement("SELECT 1, current_query() as my_column")

    out, err, status = test.run()
    assert_expected_res(out, "SELECT 1, current_query() as my_column", status, err)


@pytest.mark.parametrize(
    ["input", "output"],
    [
        ("select LIST_VALUE(1, 2)", "[1, 2]"),
        ("select STRUCT_PACK(x := 3, y := 3)", "{'x': 3, 'y': 3}"),
        ("select STRUCT_PACK(x := 3, y := LIST_VALUE(1, 2))", "{'x': 3, 'y': [1, 2]}"),
    ],
)
def test_nested_types(shell, input, output):
    test = ShellTest(shell).statement(input)
    out, err, status = test.run()
    assert_expected_res(out, output, status, err)


def test_invalid_cast(shell):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE a (i STRING)")
        .statement("INSERT INTO a VALUES ('XXXX')")
        .statement("SELECT CAST(i AS INTEGER) FROM a")
    )
    out, err, status = test.run()
    assert_expected_err(out, "Could not convert", status, err)


@pytest.mark.parametrize(
    ["input", "error"],
    [
        (".auth ON", "sqlite3_set_authorizer"),
        (".auth OFF", "sqlite3_set_authorizer"),
    ],
)
def test_invalid_shell_commands(shell, input, error):
    test = ShellTest(shell).statement(input)
    out, err, status = test.run()
    assert_expected_err(out, error, status, err)


def test_invalid_backup(shell, random_filepath):
    random_filepath = str(random_filepath).replace('\\', '/')
    test = ShellTest(shell).statement(f'.backup {random_filepath}')
    out, err, status = test.run()
    assert_expected_err(out, "sqlite3_backup_init", status, err)

def test_newline_in_value(shell):
    test = (
        ShellTest(shell)
        .statement("""select 'hello
world' as a""")
    )

    out, err, status = test.run()
    assert_expected_res(out, "hello\\nworld", status, err)

def test_newline_in_column_name(shell):
    test = (
        ShellTest(shell)
        .statement("""select 42 as "hello
world" """)
    )

    out, err, status = test.run()
    assert_expected_res(out, "hello\\nworld", status, err)

# FIXME: this test was underspecified, no expected result was provided
def test_bailing_mechanism(shell):
    test = (
        ShellTest(shell)
        .statement(".bail on")
        .statement(".bail off")
        .statement(".binary on")
        .statement("SELECT 42")
        .statement(".binary off")
        .statement("SELECT 42")
    )

    out, err, status = test.run()
    assert_expected_res(out, "42", status, err)

# FIXME: no verification at all?
def test_cd(shell, tmp_path):
    import os
    current_dir = os.getcwd()

    test = (
        ShellTest(shell)
        .statement(f".cd {tmp_path}")
        .statement(f".cd {current_dir}")
    )
    _, _, _ = test.run()

def test_changes_on(shell):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE a (I INTEGER)")
        .statement(".changes on")
        .statement("INSERT INTO a VALUES (42)")
        .statement("INSERT INTO a VALUES (42)")
        .statement("INSERT INTO a VALUES (42)")
        .statement("DROP TABLE a")
    )
    out, err, status = test.run()
    assert_expected_res(out, "total_changes: 3", status, err)

def test_changes_off(shell):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE a (I INTEGER);")
        .statement(".changes off")
        .statement("INSERT INTO a VALUES (42);")
        .statement("DROP TABLE a;")
    )
    out, err, status = test.run()
    assert_expected_res(out, "", status, err)

def test_echo(shell):
    test = (
        ShellTest(shell)
        .statement(".echo on")
        .statement("SELECT 42;")
    )
    out, err, status = test.run()
    assert_expected_res(out, "SELECT 42", status, err)

@pytest.mark.parametrize("alias", ["exit", "quit"])
def test_exit(shell, alias):
    test = ShellTest(shell).statement(f".{alias}")
    _, _, _ = test.run()

def test_print(shell):
    test = ShellTest(shell).statement(".print asdf")
    out, err, status = test.run()
    assert_expected_res(out, "asdf", status, err)

def test_headers(shell):
    test = (
        ShellTest(shell)
        .statement(".headers on")
        .statement("SELECT 42 as wilbur")
    )
    out, err, status = test.run()
    assert_expected_res(out, "wilbur", status, err)

def test_like(shell):
    test = ShellTest(shell).statement("select 'yo' where 'abc' like 'a%c'")
    out, err, status = test.run()
    assert_expected_res(out, "yo", status, err)

def test_regexp_matches(shell):
    test = ShellTest(shell).statement("select regexp_matches('abc','abc')")
    out, err, status = test.run()
    assert_expected_res(out, "true", status, err)

def test_help(shell):
    test = (
        ShellTest(shell).
        statement(".help")
    )
    out, err, status = test.run()
    assert_expected_res(out, "Show help text for PATTERN", status, err)

def test_load_error(shell, random_filepath):
    test = (
        ShellTest(shell)
        .statement(f".load {random_filepath}")
    )
    out, err, status = test.run()
    assert_expected_err(out, "Error", status, err)

def test_streaming_error(shell):
    test = (
        ShellTest(shell)
        .statement("SELECT x::INT FROM (SELECT x::VARCHAR x FROM range(10) tbl(x) UNION ALL SELECT 'hello' x) tbl(x);")
    )
    out, err, status = test.run()
    assert_expected_err(out, "Could not convert string", status, err)

@pytest.mark.parametrize("stmt", [
    "explain select sum(i) from range(1000) tbl(i)",
    "explain analyze select sum(i) from range(1000) tbl(i)"
])
def test_explain(shell, stmt):
    test = (
        ShellTest(shell)
        .statement(stmt)
    )
    out, err, status = test.run()
    assert_expected_res(out, "RANGE", status, err)

def test_returning_insert(shell):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE table1 (a INTEGER DEFAULT -1, b INTEGER DEFAULT -2, c INTEGER DEFAULT -3);")
        .statement("INSERT INTO table1 VALUES (1, 2, 3) RETURNING *;")
        .statement("SELECT COUNT(*) FROM table1;")
    )
    out, err, status = test.run()
    assert_expected_res(out, "1", status, err)

def test_pragma_display(shell):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE table1 (mylittlecolumn INTEGER);")
        .statement("pragma table_info('table1');")
    )
    out, err, status = test.run()
    assert_expected_res(out, "mylittlecolumn", status, err)

def test_show_display(shell):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE table1 (mylittlecolumn INTEGER);")
        .statement("show table1;")
    )
    out, err, status = test.run()
    assert_expected_res(out, "mylittlecolumn", status, err)

def test_call_display(shell):
    test = (
        ShellTest(shell)
        .statement("CALL range(4);")
    )
    out, err, status = test.run()
    assert_expected_res(out, "3", status, err)

def test_execute_display(shell):
    test = (
        ShellTest(shell)
        .statement("PREPARE v1 AS SELECT ?::INT;")
        .statement("EXECUTE v1(42);")
    )
    out, err, status = test.run()
    assert_expected_res(out, "42", status, err)

# this should be fixed
def test_selftest(shell):
    test = (
        ShellTest(shell)
        .statement(".selftest")
    )
    out, err, status = test.run()
    assert_expected_err(out, "sqlite3_table_column_metadata", status, err)

@pytest.mark.parametrize('generated_file', ["select 42"], indirect=True)
def test_read(shell, generated_file):
    test = (
        ShellTest(shell)
        .statement(f".read {generated_file}")
    )
    out, err, status = test.run()
    assert_expected_res(out, "42", status, err)

def test_show_basic(shell):
    test = (
        ShellTest(shell)
        .statement(".show")
    )
    out, err, status = test.run()
    assert_expected_res(out, "rowseparator", status, err)

def test_limit_error(shell):
    test = (
        ShellTest(shell)
        .statement(".limit length 42")
    )
    out, err, status = test.run()
    assert_expected_err(out, "sqlite3_limit", status, err)

def test_timeout(shell):
    test = (
        ShellTest(shell)
        .statement(".timeout")
    )
    out, err, status = test.run()
    assert_expected_err(out, "sqlite3_busy_timeout", status, err)


def test_save(shell, random_filepath):
    test = (
        ShellTest(shell)
        .statement(f".save {random_filepath}")
    )
    out, err, status = test.run()
    assert_expected_err(out, "sqlite3_backup_init", status, err)

def test_restore(shell, random_filepath):
    test = (
        ShellTest(shell)
        .statement(f".restore {random_filepath}")
    )
    out, err, status = test.run()
    assert_expected_err(out, "sqlite3_backup_init", status, err)

@pytest.mark.parametrize("cmd", [
    ".vfsinfo",
    ".vfsname",
    ".vfslist"
])
def test_volatile_commands(shell, cmd):
    # The original comment read: don't crash plz
    test = (
        ShellTest(shell)
        .statement(f".{cmd}")
    )
    out, err, status = test.run()
    assert_expected_err(out, "", status, err)

def test_stats_error(shell):
    test = (
        ShellTest(shell)
        .statement(".stats")
    )
    out, err, status = test.run()
    assert_expected_err(out, "sqlite3_status64", status, err)

@pytest.mark.parametrize("param", [
    "off",
    "on"
])
def test_stats(shell, param):
    test = (
        ShellTest(shell)
        .statement(f".stats {param}")
    )
    out, err, status = test.run()
    assert_expected_res(out, "", status, err)

@pytest.mark.parametrize("pattern", [
    "test",
    "tes%",
    "tes*",
    ""
])
def test_schema(shell, pattern):
    test = (
        ShellTest(shell)
        .statement("create table test (a int, b varchar);")
        .statement("insert into test values (1, 'hello');")
        .statement(f".schema {pattern}")
    )
    out, err, status = test.run()
    assert_expected_res(out, "CREATE TABLE test(a INTEGER, b VARCHAR);", status, err)

def test_tables(shell):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE asda (i INTEGER);")
        .statement("CREATE TABLE bsdf (i INTEGER);")
        .statement("CREATE TABLE csda (i INTEGER);")
        .statement(".tables")
    )
    out, err, status = test.run()
    assert_expected_res(out, "asda  bsdf  csda", status, err)

def test_tables_pattern(shell):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE asda (i INTEGER);")
        .statement("CREATE TABLE bsdf (i INTEGER);")
        .statement("CREATE TABLE csda (i INTEGER);")
        .statement(".tables %da")
    )
    out, err, status = test.run()
    assert_expected_res(out, "asda  csda", status, err)

def test_indexes(shell):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE a (i INTEGER);")
        .statement("CREATE INDEX a_idx ON a(i);")
        .statement(".indexes a%")
    )
    out, err, status = test.run()
    assert_expected_res(out, "a_idx", status, err)

def test_schema_pattern_no_result(shell):
    test = (
        ShellTest(shell)
        .statement(".schema %p%")
    )
    out, err, status = test.run()
    assert_expected_res(out, "", status, err)

def test_schema_pattern(shell):
    test = (
        ShellTest(shell)
        .statement("create table duckdb_p (a int, b varchar, c BIT);")
        .statement("create table p_duck(d INT, f DATE);")
        .statement(".schema %p")
    )
    out, err, status = test.run()
    assert_expected_res(out, "CREATE TABLE duckdb_p(a INTEGER, b VARCHAR, c BIT);", status, err)

@pytest.mark.skipif(os.name == 'nt', reason="Windows treats newlines in a problematic manner")
def test_schema_pattern_extended(shell):
    test = (
        ShellTest(shell)
        .statement("create table duckdb_p (a int, b varchar, c BIT);")
        .statement("create table p_duck(d INT, f DATE);")
        .statement(".schema %p%")
    )
    out, err, status = test.run()
    expected = [
        "CREATE TABLE duckdb_p(a INTEGER, b VARCHAR, c BIT);",
        "CREATE TABLE p_duck(d INTEGER, f DATE);"
    ]
    assert_expected_res(out, expected, status, err)

def test_clone_error(shell):
    test = (
        ShellTest(shell)
        .statement(".clone")
    )
    out, err, status = test.run()
    assert_expected_err(out, 'Error: unknown command or invalid arguments:  "clone". Enter ".help" for help', status, err)

def test_sha3sum(shell):
    test = (
        ShellTest(shell)
        .statement(".sha3sum")
    )
    out, err, status = test.run()
    assert_expected_err(out, '', status, err)

def test_jsonlines(shell):
    test = (
        ShellTest(shell)
        .statement(".mode jsonlines")
        .statement("SELECT 42,43;")
    )
    out, err, status = test.run()
    assert_expected_res(out, '{"42":42,"43":43}', status, err)

def test_separator(shell):
    test = (
        ShellTest(shell)
        .statement(".mode csv")
        .statement(".separator XX")
        .statement("SELECT 42,43;")
    )
    out, err, status = test.run()
    assert_expected_res(out, '42XX43', status, err)

def test_timer(shell):
    test = (
        ShellTest(shell)
        .statement(".timer on")
        .statement("SELECT NULL;")
    )
    out, err, status = test.run()
    assert_expected_res(out, 'Run Time (s):', status, err)


def test_scanstats(shell):
    test = (
        ShellTest(shell)
        .statement(".scanstats on")
        .statement("SELECT NULL;")
    )
    out, err, status = test.run()
    assert_expected_err(out, 'scanstats', status, err)

def test_trace(shell, random_filepath):
    test = (
        ShellTest(shell)
        .statement(f".trace {random_filepath}")
        .statement("SELECT 42;")
    )
    out, err, status = test.run()
    assert_expected_err(out, 'sqlite3_trace_v2', status, err)
    
def test_output_csv_mode(shell, random_filepath):
    test = (
        ShellTest(shell)
        .statement(".mode csv")
        .statement(f".output {random_filepath}")
        .statement("SELECT 42;")
    )
    out, err, status = test.run()
    outstr = open(random_filepath, 'rb').read()
    assert_expected_res(outstr, b'42', status, err)

def test_issue_6204(shell):
    test = (
        ShellTest(shell)
        .statement(".output foo.txt")
        .statement("select * from range(2049);")
    )
    out, err, status = test.run()
    assert_expected_res(out, "", status, err)

def test_once(shell, random_filepath):
    test = (
        ShellTest(shell)
        .statement(f".once {random_filepath}")
        .statement("SELECT 43;")
    )
    out, err, status = test.run()
    outstr = open(random_filepath, 'rb').read()
    assert_expected_res(outstr, b'43', status, err)

def test_log(shell, random_filepath):
    test = (
        ShellTest(shell)
        .statement(f".log {random_filepath}")
        .statement("SELECT 42;")
        .statement(".log off")
    )
    out, err, status = test.run()
    assert_expected_res(out, '', status, err)

def test_mode_ascii(shell):
    test = (
        ShellTest(shell)
        .statement(".mode ascii")
        .statement("SELECT NULL, 42, 'fourty-two', 42.0;")
    )
    out, err, status = test.run()
    assert_expected_res(out, 'fourty-two', status, err)

def test_mode_csv(shell):
    test = (
        ShellTest(shell)
        .statement(".mode csv")
        .statement("SELECT NULL, 42, 'fourty-two', 42.0;")
    )
    out, err, status = test.run()
    assert_expected_res(out, ',fourty-two,', status, err)

def test_mode_column(shell):
    test = (
        ShellTest(shell)
        .statement(".mode column")
        .statement("SELECT NULL, 42, 'fourty-two', 42.0;")
    )
    out, err, status = test.run()
    assert_expected_res(out, '  fourty-two  ', status, err)

def test_mode_html(shell):
    test = (
        ShellTest(shell)
        .statement(".mode html")
        .statement("SELECT NULL, 42, 'fourty-two', 42.0;")
    )
    out, err, status = test.run()
    assert_expected_res(out, '<TD>fourty-two</TD>', status, err)

# Original comment: FIXME sqlite3_column_blob
def test_mode_insert(shell):
    test = (
        ShellTest(shell)
        .statement(".mode insert")
        .statement("SELECT NULL, 42, 'fourty-two', 42.0;")
    )
    out, err, status = test.run()
    assert_expected_res(out, 'fourty-two', status, err)

def test_mode_line(shell):
    test = (
        ShellTest(shell)
        .statement(".mode line")
        .statement("SELECT NULL, 42, 'fourty-two' x, 42.0;")
    )
    out, err, status = test.run()
    assert_expected_res(out, 'x = fourty-two', status, err)

def test_mode_list(shell):
    test = (
        ShellTest(shell)
        .statement(".mode list")
        .statement("SELECT NULL, 42, 'fourty-two' x, 42.0;")
    )
    out, err, status = test.run()
    assert_expected_res(out, '|fourty-two|', status, err)

# Original comment: FIXME sqlite3_column_blob and %! format specifier
def test_mode_quote(shell):
    test = (
        ShellTest(shell)
        .statement(".mode quote")
        .statement("SELECT NULL, 42, 'fourty-two' x, 42.0;")
    )
    out, err, status = test.run()
    assert_expected_res(out, 'fourty-two', status, err)

def test_mode_tabs(shell):
    test = (
        ShellTest(shell)
        .statement(".mode tabs")
        .statement("SELECT NULL, 42, 'fourty-two' x, 42.0;")
    )
    out, err, status = test.run()
    assert_expected_res(out, 'fourty-two', status, err)

def test_open(shell, tmp_path):
    file_one = tmp_path / "file_one"
    file_two = tmp_path / "file_two"
    test = (
        ShellTest(shell)
        .statement(f".open {file_one}")
        .statement("CREATE TABLE t1 (i INTEGER);")
        .statement("INSERT INTO t1 VALUES (42);")
        .statement(f".open {file_two}")
        .statement("CREATE TABLE t2 (i INTEGER);")
        .statement("INSERT INTO t2 VALUES (43);")
        .statement(f".open {file_one}")
        .statement("SELECT * FROM t1;")
    )
    out, err, status = test.run()
    assert_expected_res(out, '42', status, err)

@pytest.mark.parametrize('generated_file', ["blablabla"], indirect=True)
def test_open_non_database(shell, generated_file):
    test = (
        ShellTest(shell)
        .add_argument(generated_file)
    )
    out, err, status = test.run()
    assert_expected_err(out, 'not a valid DuckDB database file', status, err)

def test_enable_profiling(shell):
    test = (
        ShellTest(shell)
        .statement("PRAGMA enable_profiling")
    )
    out, err, status = test.run()
    assert_expected_err(out, '', status, err)

def test_profiling_select(shell):
    test = (
        ShellTest(shell)
        .statement("PRAGMA enable_profiling")
        .statement("select 42")
    )
    out, err, status = test.run()
    assert_expected_err(out, 'Query Profiling Information', status, err)
    assert_expected_res(out, '42', status, err)

@pytest.mark.parametrize("command", [
    "system",
    "shell"
])
def test_echo_command(shell, command):
    test = (
        ShellTest(shell)
        .statement(f".{command} echo 42")
    )
    out, err, status = test.run()
    assert_expected_res(out, '42', status, err)

def test_profiling_optimizer(shell):
    test = (
        ShellTest(shell)
        .statement("PRAGMA enable_profiling=query_tree_optimizer;")
        .statement("SELECT 42;")
    )
    out, err, status = test.run()
    assert_expected_err(out, 'Optimizer', status, err)
    assert_expected_res(out, '42', status, err)

def test_profiling_optimizer_detailed(shell):
    test = (
        ShellTest(shell)
        .statement("PRAGMA enable_profiling;")
        .statement("PRAGMA profiling_mode=detailed;")
        .statement("SELECT 42;")
    )
    out, err, status = test.run()
    assert_expected_err(out, 'Optimizer', status, err)
    assert_expected_res(out, '42', status, err)

def test_profiling_optimizer_json(shell):
    test = (
        ShellTest(shell)
        .statement("PRAGMA enable_profiling=json;")
        .statement("PRAGMA profiling_mode=detailed;")
        .statement("SELECT 42;")
    )
    out, err, status = test.run()
    assert_expected_err(out, 'optimizer', status, err)
    assert_expected_res(out, '42', status, err)


# Original comment: this fails because db_config is missing
@pytest.mark.skip(reason="db_config is not supported (yet?)")
def test_eqp(shell):
    test = (
        ShellTest(shell)
        .statement(".eqp full")
        .statement("SELECT 42;")
    )
    out, err, status = test.run()
    assert_expected_res(out, 'DUMMY_SCAN', status, err)

def test_clone(shell, random_filepath):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE a (I INTEGER)")
        .statement("INSERT INTO a VALUES (42)")
        .statement(f".clone {random_filepath}")
    )
    out, err, status = test.run()
    assert_expected_err(out, 'unknown command or invalid arguments', status, err)


def test_databases(shell):
    test = (
        ShellTest(shell)
        .statement(".databases")
    )
    out, err, status = test.run()
    assert_expected_res(out, 'memory', status, err)


def test_dump_create(shell):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE a (i INTEGER);")
        .statement(".changes off")
        .statement("INSERT INTO a VALUES (42);")
        .statement(".dump")
    )
    out, err, status = test.run()
    assert_expected_res(out, 'CREATE TABLE a(i INTEGER)', status, err)
    assert_expected_res(out, 'COMMIT', status, err)

@pytest.mark.parametrize("pattern", [
    "a",
    "a%"
])
def test_dump_specific(shell, pattern):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE a (i INTEGER);")
        .statement(".changes off")
        .statement("INSERT INTO a VALUES (42);")
        .statement(f".dump {pattern}")
    )
    out, err, status = test.run()
    assert_expected_res(out, 'CREATE TABLE a(i INTEGER)', status, err)

# Original comment: more types, tables and views
def test_dump_mixed(shell):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE a (d DATE, k FLOAT, t TIMESTAMP);")
        .statement("CREATE TABLE b (c INTEGER);")
        .statement(".changes off")
        .statement("INSERT INTO a VALUES (DATE '1992-01-01', 0.3, NOW());")
        .statement("INSERT INTO b SELECT * FROM range(0,10);")
        .statement(".dump")
    )
    out, err, status = test.run()
    assert_expected_res(out, 'CREATE TABLE a(d DATE, k FLOAT, t TIMESTAMP);', status, err)

def test_invalid_csv(shell, tmp_path):
    file = tmp_path / 'nonsencsv.csv'
    with open(file, 'wb+') as f:
        f.write(b'\xFF\n')
    test = (
        ShellTest(shell)
        .statement(".nullvalue NULL")
        .statement("CREATE TABLE test(i INTEGER);")
        .statement(f".import {file} test")
        .statement("SELECT * FROM test;")
    )
    out, err, status = test.run()
    assert_expected_res(out, 'NULL', status, err)

def test_mode_latex(shell):
    test = (
        ShellTest(shell)
        .statement(".mode latex")
        .statement("CREATE TABLE a (I INTEGER);")
        .statement(".changes off")
        .statement("INSERT INTO a VALUES (42);")
        .statement("SELECT * FROM a;")
    )
    out, err, status = test.run()
    assert_expected_res(out, '\\begin{tabular}', status, err)

def test_mode_trash(shell):
    test = (
        ShellTest(shell)
        .statement(".mode trash")
        .statement("select 1")
    )
    out, err, status = test.run()
    assert_expected_res(out, '', status, err)

@pytest.mark.skip(reason="Broken test, ported directly, was commented out")
def test_dump_blobs(shell):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE a (b BLOB);")
        .statement(".changes off")
        .statement("INSERT INTO a VALUES (DATE '1992-01-01', 0.3, NOW());")
        .statement(".dump")
    )
    out, err, status = test.run()
    assert_expected_res(out, 'COMMIT', status, err)

def test_sqlite_comments(shell):
    # Using /* <comment> */
    test = (
        ShellTest(shell)
        .statement("""/*
;
*/""")
        .statement("select 42")
    )
    out, err, status = test.run()
    assert_expected_res(out, '42', status, err)

    # Using -- <comment>
    test = (
        ShellTest(shell)
        .statement("""-- this is a comment ;
select 42;
""")
    )
    out, err, status = test.run()
    assert_expected_res(out, '42', status, err)

    # More extreme: -- <comment>
    test = (
        ShellTest(shell)
        .statement("""--;;;;;;
select 42;
""")
    )
    out, err, status = test.run()
    assert_expected_res(out, '42', status, err)

    # More extreme: /* <comment> */
    test = (
        ShellTest(shell)
        .statement('/* ;;;;;; */ select 42;')
    )
    out, err, status = test.run()
    assert_expected_res(out, '42', status, err)

def test_sqlite_udfs_error(shell):
    test = (
        ShellTest(shell)
        .statement("SELECT writefile()")
    )
    out, err, status = test.run()
    assert_expected_err(out, 'wrong number of arguments to function writefile', status, err)

    test = (
        ShellTest(shell)
        .statement("SELECT writefile('hello')")
    )
    out, err, status = test.run()
    assert_expected_err(out, 'wrong number of arguments to function writefile', status, err)

def test_sqlite_udfs_correct(shell, random_filepath):
    import os
    test = (
        ShellTest(shell)
        .statement(f"SELECT writefile('{random_filepath}', 'hello');")
    )
    out, err, status = test.run()
    if not os.path.exists(random_filepath):
        raise Exception(f"Failed to write file {random_filepath}")
    with open(random_filepath, 'r') as f:
        text = f.read()
    assert_expected_res(text, 'hello', status, err)

def test_lsmode(shell):
    test = (
        ShellTest(shell)
        .statement("SELECT lsmode(1) AS lsmode;")
    )
    out, err, status = test.run()
    assert_expected_res(out, 'lsmode', status, err)

def test_duckbox(shell):
    test = (
        ShellTest(shell)
        .statement(".mode duckbox")
        .statement("select 42 limit 0;")
    )
    out, err, status = test.run()
    assert_expected_res(out, '0 rows', status, err)

# Original comment: #5411 - with maxrows=2, we still display all 4 rows (hiding them would take up more space)
def test_maxrows(shell):
    test = (
        ShellTest(shell)
        .statement(".maxrows 2")
        .statement("select * from range(4);")
    )
    out, err, status = test.run()
    assert_expected_res(out, '1', status, err)

def test_maxrows_outfile(shell, random_filepath):
    file = random_filepath
    test = (
        ShellTest(shell)
        .statement(".maxrows 2")
        .statement(f".output {file}")
        .statement("SELECT * FROM range(100);")
    )
    out, err, status = test.run()
    outstr = open(file, 'rb').read().decode('utf8')
    assert_expected_res(outstr, '50', status, err)

def test_columns_to_file(shell, random_filepath):
    columns = ', '.join([str(x) for x in range(100)])
    test = (
        ShellTest(shell)
        .statement(f".output {random_filepath}")
        .statement(f"SELECT {columns}")
    )
    out, err, status = test.run()
    outstr = open(random_filepath, 'rb').read().decode('utf8')
    assert_expected_res(outstr, '99', status, err)

def test_columnar_mode(shell):
    test = (
        ShellTest(shell)
        .statement(".col")
        .statement("select * from range(4);")
    )
    out, err, status = test.run()
    assert_expected_res(out, 'Row 1', status, err)

def test_columnar_mode_constant(shell):
    columns = ','.join(["'MyValue" + str(x) + "'" for x in range(100)])
    test = (
        ShellTest(shell)
        .statement(".col")
        .statement(f"select {columns};")
    )
    out, err, status = test.run()
    assert_expected_res(out, 'MyValue50', status, err)

    test = (
        ShellTest(shell)
        .statement(".col")
        .statement(f"select {columns} from range(1000);")
    )
    out, err, status = test.run()
    assert_expected_res(out, '100 columns', status, err)

def test_nullbyte_rendering(shell):
    test = (
        ShellTest(shell)
        .statement("select varchar from test_all_types();")
    )
    out, err, status = test.run()
    assert_expected_res(out, 'goo\\0se', status, err)

def test_nullbyte_error_rendering(shell):
    test = (
        ShellTest(shell)
        .statement("select chr(0)::int")
    )
    out, err, status = test.run()
    assert_expected_err(out, 'INT32', status, err)

@pytest.mark.parametrize("stmt", [
	"select decimal_mul(NULL, NULL);",
	"select decimal_mul(NULL, i) FROM range(3) t(i);",
	"select sha3(NULL);"
])
def test_sqlite_udf_null(shell, stmt):
    test = (
        ShellTest(shell)
        .statement(stmt)
    )
    out, err, status = test.run()
    assert_expected_res(out, 'NULL', status, err)

def test_sqlite_udf_sha3_int(shell):
    test = (
        ShellTest(shell)
        .statement("select sha3(256)")
    )
    out, err, status = test.run()
    assert_expected_res(out, 'A7', status, err)

def test_sqlite_udf_sha3_non_inlined_string(shell):
    test = (
        ShellTest(shell)
        .statement("select sha3('hello world this is a long string');")
    )
    out, err, status = test.run()
    assert_expected_res(out, 'D4', status, err)

# fmt: on
