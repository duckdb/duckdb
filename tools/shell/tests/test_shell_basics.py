# fmt: off

import pytest
import subprocess
import sys
from typing import List
from conftest import ShellTest
import os
from pathlib import Path


def test_basic(shell):
    test = ShellTest(shell).statement("select 'asdf' as a")
    result = test.run()
    result.check_stdout("asdf")


def test_range(shell):
    test = ShellTest(shell).statement("select * from range(10000)")
    result = test.run()
    result.check_stdout("9999")


@pytest.mark.parametrize('generated_file', ["col_1,col_2\n1,2\n10,20"], indirect=True)
def test_import(shell, generated_file):
    test = (
        ShellTest(shell)
        .statement(".mode csv")
        .statement(f'.import "{generated_file.as_posix()}" test_table')
        .statement("select * FROM test_table")
    )

    result = test.run()
    result.check_stdout("col_1,col_2\n1,2\n10,20")


@pytest.mark.parametrize('generated_file', ["42\n84"], indirect=True)
def test_import_sum(shell, generated_file):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE a (i INTEGER)")
        .statement(f'.import "{generated_file.as_posix()}" a')
        .statement("SELECT SUM(i) FROM a")
    )

    result = test.run()
    result.check_stdout("126")


def test_pragma(shell):
    test = (
        ShellTest(shell)
        .statement(".mode csv")
        .statement(".headers off")
        .statement(".sep |")
        .statement("CREATE TABLE t0(c0 INT);")
        .statement("PRAGMA table_info('t0');")
    )

    result = test.run()
    result.check_stdout("0|c0|INTEGER|false||false")


def test_system_functions(shell):
    test = ShellTest(shell).statement("SELECT 1, current_query() as my_column")

    result = test.run()
    result.check_stdout("SELECT 1, current_query() as my_column")


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
    result = test.run()
    result.check_stdout(output)


def test_invalid_cast(shell):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE a (i STRING)")
        .statement("INSERT INTO a VALUES ('XXXX')")
        .statement("SELECT CAST(i AS INTEGER) FROM a")
    )
    result = test.run()
    result.check_stderr("Could not convert")


@pytest.mark.parametrize(
    ["input", "error"],
    [
        (".auth ON", "sqlite3_set_authorizer"),
        (".auth OFF", "sqlite3_set_authorizer"),
    ],
)
def test_invalid_shell_commands(shell, input, error):
    test = ShellTest(shell).statement(input)
    result = test.run()
    result.check_stderr(error)


def test_invalid_backup(shell, random_filepath):
    test = ShellTest(shell).statement(f'.backup {random_filepath.as_posix()}')
    result = test.run()
    result.check_stderr("sqlite3_backup_init")

def test_newline_in_value(shell):
    test = (
        ShellTest(shell)
        .statement("""select 'hello
world' as a""")
    )

    result = test.run()
    result.check_stdout("hello\\nworld")

def test_newline_in_column_name(shell):
    test = (
        ShellTest(shell)
        .statement("""select 42 as "hello
world" """)
    )

    result = test.run()
    result.check_stdout("hello\\nworld")

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

    result = test.run()
    result.check_stdout("42")

# FIXME: no verification at all?
def test_cd(shell, tmp_path):
    current_dir = Path(os.getcwd())

    test = (
        ShellTest(shell)
        .statement(f".cd {tmp_path.as_posix()}")
        .statement(f".cd {current_dir.as_posix()}")
    )
    result = test.run()

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
    result = test.run()
    result.check_stdout("total_changes: 3")

def test_changes_off(shell):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE a (I INTEGER);")
        .statement(".changes off")
        .statement("INSERT INTO a VALUES (42);")
        .statement("DROP TABLE a;")
    )
    result = test.run()
    result.check_stdout("")

def test_echo(shell):
    test = (
        ShellTest(shell)
        .statement(".echo on")
        .statement("SELECT 42;")
    )
    result = test.run()
    result.check_stdout("SELECT 42")

@pytest.mark.parametrize("alias", ["exit", "quit"])
def test_exit(shell, alias):
    test = ShellTest(shell).statement(f".{alias}")
    result = test.run()

def test_print(shell):
    test = ShellTest(shell).statement(".print asdf")
    result = test.run()
    result.check_stdout("asdf")

def test_headers(shell):
    test = (
        ShellTest(shell)
        .statement(".headers on")
        .statement("SELECT 42 as wilbur")
    )
    result = test.run()
    result.check_stdout("wilbur")

def test_like(shell):
    test = ShellTest(shell).statement("select 'yo' where 'abc' like 'a%c'")
    result = test.run()
    result.check_stdout("yo")

def test_regexp_matches(shell):
    test = ShellTest(shell).statement("select regexp_matches('abc','abc')")
    result = test.run()
    result.check_stdout("true")

def test_help(shell):
    test = (
        ShellTest(shell).
        statement(".help")
    )
    result = test.run()
    result.check_stdout("Show help text for PATTERN")

def test_load_error(shell, random_filepath):
    test = (
        ShellTest(shell)
        .statement(f".load {random_filepath.as_posix()}")
    )
    result = test.run()
    result.check_stderr("Error")

def test_streaming_error(shell):
    test = (
        ShellTest(shell)
        .statement("SELECT x::INT FROM (SELECT x::VARCHAR x FROM range(10) tbl(x) UNION ALL SELECT 'hello' x) tbl(x);")
    )
    result = test.run()
    result.check_stderr("Could not convert string")

@pytest.mark.parametrize("stmt", [
    "explain select sum(i) from range(1000) tbl(i)",
    "explain analyze select sum(i) from range(1000) tbl(i)"
])
def test_explain(shell, stmt):
    test = (
        ShellTest(shell)
        .statement(stmt)
    )
    result = test.run()
    result.check_stdout("RANGE")

def test_returning_insert(shell):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE table1 (a INTEGER DEFAULT -1, b INTEGER DEFAULT -2, c INTEGER DEFAULT -3);")
        .statement("INSERT INTO table1 VALUES (1, 2, 3) RETURNING *;")
        .statement("SELECT COUNT(*) FROM table1;")
    )
    result = test.run()
    result.check_stdout("1")

def test_pragma_display(shell):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE table1 (mylittlecolumn INTEGER);")
        .statement("pragma table_info('table1');")
    )
    result = test.run()
    result.check_stdout("mylittlecolumn")

def test_show_display(shell):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE table1 (mylittlecolumn INTEGER);")
        .statement("show table1;")
    )
    result = test.run()
    result.check_stdout("mylittlecolumn")

def test_call_display(shell):
    test = (
        ShellTest(shell)
        .statement("CALL range(4);")
    )
    result = test.run()
    result.check_stdout("3")

def test_execute_display(shell):
    test = (
        ShellTest(shell)
        .statement("PREPARE v1 AS SELECT ?::INT;")
        .statement("EXECUTE v1(42);")
    )
    result = test.run()
    result.check_stdout("42")

# this should be fixed
def test_selftest(shell):
    test = (
        ShellTest(shell)
        .statement(".selftest")
    )
    result = test.run()
    result.check_stderr("sqlite3_table_column_metadata")

@pytest.mark.parametrize('generated_file', ["select 42"], indirect=True)
def test_read(shell, generated_file):
    test = (
        ShellTest(shell)
        .statement(f".read {generated_file.as_posix()}")
    )
    result = test.run()
    result.check_stdout("42")

def test_show_basic(shell):
    test = (
        ShellTest(shell)
        .statement(".show")
    )
    result = test.run()
    result.check_stdout("rowseparator")

def test_timeout(shell):
    test = (
        ShellTest(shell)
        .statement(".timeout")
    )
    result = test.run()
    result.check_stderr("sqlite3_busy_timeout")


def test_save(shell, random_filepath):
    test = (
        ShellTest(shell)
        .statement(f".save {random_filepath.as_posix()}")
    )
    result = test.run()
    result.check_stderr("sqlite3_backup_init")

def test_restore(shell, random_filepath):
    test = (
        ShellTest(shell)
        .statement(f".restore {random_filepath.as_posix()}")
    )
    result = test.run()
    result.check_stderr("sqlite3_backup_init")

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
    result = test.run()
    result.check_stderr("")

def test_stats_error(shell):
    test = (
        ShellTest(shell)
        .statement(".stats")
    )
    result = test.run()
    result.check_stderr("sqlite3_status64")

@pytest.mark.parametrize("param", [
    "off",
    "on"
])
def test_stats(shell, param):
    test = (
        ShellTest(shell)
        .statement(f".stats {param}")
    )
    result = test.run()
    result.check_stdout("")

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
    result = test.run()
    result.check_stdout("CREATE TABLE test(a INTEGER, b VARCHAR);")

def test_tables(shell):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE asda (i INTEGER);")
        .statement("CREATE TABLE bsdf (i INTEGER);")
        .statement("CREATE TABLE csda (i INTEGER);")
        .statement(".tables")
    )
    result = test.run()
    result.check_stdout("asda  bsdf  csda")

def test_tables_pattern(shell):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE asda (i INTEGER);")
        .statement("CREATE TABLE bsdf (i INTEGER);")
        .statement("CREATE TABLE csda (i INTEGER);")
        .statement(".tables %da")
    )
    result = test.run()
    result.check_stdout("asda  csda")

def test_indexes(shell):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE a (i INTEGER);")
        .statement("CREATE INDEX a_idx ON a(i);")
        .statement(".indexes a%")
    )
    result = test.run()
    result.check_stdout("a_idx")

def test_schema_pattern_no_result(shell):
    test = (
        ShellTest(shell)
        .statement(".schema %p%")
    )
    result = test.run()
    result.check_stdout("")

def test_schema_pattern(shell):
    test = (
        ShellTest(shell)
        .statement("create table duckdb_p (a int, b varchar, c BIT);")
        .statement("create table p_duck(d INT, f DATE);")
        .statement(".schema %p")
    )
    result = test.run()
    result.check_stdout("CREATE TABLE duckdb_p(a INTEGER, b VARCHAR, c BIT);")

@pytest.mark.skipif(os.name == 'nt', reason="Windows treats newlines in a problematic manner")
def test_schema_pattern_extended(shell):
    test = (
        ShellTest(shell)
        .statement("create table duckdb_p (a int, b varchar, c BIT);")
        .statement("create table p_duck(d INT, f DATE);")
        .statement(".schema %p%")
    )
    result = test.run()
    expected = [
        "CREATE TABLE duckdb_p(a INTEGER, b VARCHAR, c BIT);",
        "CREATE TABLE p_duck(d INTEGER, f DATE);"
    ]
    result.check_stdout(expected)

def test_clone_error(shell):
    test = (
        ShellTest(shell)
        .statement(".clone")
    )
    result = test.run()
    result.check_stderr('Error: unknown command or invalid arguments:  "clone". Enter ".help" for help')

def test_sha3sum(shell):
    test = (
        ShellTest(shell)
        .statement(".sha3sum")
    )
    result = test.run()
    result.check_stderr('')

def test_jsonlines(shell):
    test = (
        ShellTest(shell)
        .statement(".mode jsonlines")
        .statement("SELECT 42,43;")
    )
    result = test.run()
    result.check_stdout('{"42":42,"43":43}')

def test_nested_jsonlines(shell, json_extension):
    test = (
        ShellTest(shell)
        .statement(".mode jsonlines")
        .statement("SELECT [1,2,3]::JSON AS x;")
    )
    result = test.run()
    result.check_stdout('{"x":[1,2,3]}')

def test_separator(shell):
    test = (
        ShellTest(shell)
        .statement(".mode csv")
        .statement(".separator XX")
        .statement("SELECT 42,43;")
    )
    result = test.run()
    result.check_stdout('42XX43')

def test_timer(shell):
    test = (
        ShellTest(shell)
        .statement(".timer on")
        .statement("SELECT NULL;")
    )
    result = test.run()
    result.check_stdout('Run Time (s):')


def test_scanstats(shell):
    test = (
        ShellTest(shell)
        .statement(".scanstats on")
        .statement("SELECT NULL;")
    )
    result = test.run()
    result.check_stderr('scanstats')

def test_trace(shell, random_filepath):
    test = (
        ShellTest(shell)
        .statement(f".trace {random_filepath.as_posix()}")
        .statement("SELECT 42;")
    )
    result = test.run()
    result.check_stderr('sqlite3_trace_v2')
    
def test_output_csv_mode(shell, random_filepath):
    test = (
        ShellTest(shell)
        .statement(".mode csv")
        .statement(f".output {random_filepath.as_posix()}")
        .statement("SELECT 42;")
    )
    result = test.run()
    result.stdout = open(random_filepath, 'rb').read()
    result.check_stdout(b'42')

def test_issue_6204(shell):
    test = (
        ShellTest(shell)
        .statement(".output foo.txt")
        .statement("select * from range(2049);")
    )
    result = test.run()
    result.check_stdout("")

def test_once(shell, random_filepath):
    test = (
        ShellTest(shell)
        .statement(f".once {random_filepath.as_posix()}")
        .statement("SELECT 43;")
    )
    result = test.run()
    result.stdout = open(random_filepath, 'rb').read()
    result.check_stdout(b'43')

def test_log(shell, random_filepath):
    test = (
        ShellTest(shell)
        .statement(f".log {random_filepath.as_posix()}")
        .statement("SELECT 42;")
        .statement(".log off")
    )
    result = test.run()
    result.check_stdout('')

def test_mode_ascii(shell):
    test = (
        ShellTest(shell)
        .statement(".mode ascii")
        .statement("SELECT NULL, 42, 'fourty-two', 42.0;")
    )
    result = test.run()
    result.check_stdout('fourty-two')

def test_mode_csv(shell):
    test = (
        ShellTest(shell)
        .statement(".mode csv")
        .statement("SELECT NULL, 42, 'fourty-two', 42.0;")
    )
    result = test.run()
    result.check_stdout(',fourty-two,')

def test_mode_column(shell):
    test = (
        ShellTest(shell)
        .statement(".mode column")
        .statement("SELECT NULL, 42, 'fourty-two', 42.0;")
    )
    result = test.run()
    result.check_stdout('  fourty-two  ')

def test_mode_html(shell):
    test = (
        ShellTest(shell)
        .statement(".mode html")
        .statement("SELECT NULL, 42, 'fourty-two', 42.0;")
    )
    result = test.run()
    result.check_stdout('<td>fourty-two</td>')

# Original comment: FIXME sqlite3_column_blob
def test_mode_insert(shell):
    test = (
        ShellTest(shell)
        .statement(".mode insert")
        .statement("SELECT NULL, 42, 'fourty-two', 42.0, 3.14, 2.71;")
    )
    result = test.run()
    result.check_stdout('fourty-two')
    result.check_stdout('3.14')
    result.check_stdout('2.71')
    result.check_not_exist('3.140000')
    result.check_not_exist('2.709999')
    result.check_not_exist('3.139999')
    result.check_not_exist('2.710000')

def test_mode_line(shell):
    test = (
        ShellTest(shell)
        .statement(".mode line")
        .statement("SELECT NULL, 42, 'fourty-two' x, 42.0;")
    )
    result = test.run()
    result.check_stdout('x = fourty-two')

def test_mode_list(shell):
    test = (
        ShellTest(shell)
        .statement(".mode list")
        .statement("SELECT NULL, 42, 'fourty-two' x, 42.0;")
    )
    result = test.run()
    result.check_stdout('|fourty-two|')

# Original comment: FIXME sqlite3_column_blob and %! format specifier
def test_mode_quote(shell):
    test = (
        ShellTest(shell)
        .statement(".mode quote")
        .statement("SELECT NULL, 42, 'fourty-two' x, 42.0;")
    )
    result = test.run()
    result.check_stdout('fourty-two')

def test_mode_tabs(shell):
    test = (
        ShellTest(shell)
        .statement(".mode tabs")
        .statement("SELECT NULL, 42, 'fourty-two' x, 42.0;")
    )
    result = test.run()
    result.check_stdout('fourty-two')

def test_open(shell, tmp_path):
    file_one = tmp_path / "file_one"
    file_two = tmp_path / "file_two"
    test = (
        ShellTest(shell)
        .statement(f".open {file_one.as_posix()}")
        .statement("CREATE TABLE t1 (i INTEGER);")
        .statement("INSERT INTO t1 VALUES (42);")
        .statement(f".open {file_two.as_posix()}")
        .statement("CREATE TABLE t2 (i INTEGER);")
        .statement("INSERT INTO t2 VALUES (43);")
        .statement(f".open {file_one.as_posix()}")
        .statement("SELECT * FROM t1;")
    )
    result = test.run()
    result.check_stdout('42')

@pytest.mark.parametrize('generated_file', ["blablabla"], indirect=True)
def test_open_non_database(shell, generated_file):
    test = (
        ShellTest(shell)
        .add_argument(generated_file.as_posix())
    )
    result = test.run()
    result.check_stderr('not a valid DuckDB database file')

def test_enable_profiling(shell):
    test = (
        ShellTest(shell)
        .statement("PRAGMA enable_profiling")
    )
    result = test.run()
    result.check_stderr('')

def test_profiling_select(shell):
    test = (
        ShellTest(shell)
        .statement("PRAGMA enable_profiling")
        .statement("select 42")
    )
    result = test.run()
    result.check_stderr('Query Profiling Information')
    result.check_stdout('42')

@pytest.mark.parametrize("command", [
    "system",
    "shell"
])
def test_echo_command(shell, command):
    test = (
        ShellTest(shell)
        .statement(f".{command} echo 42")
    )
    result = test.run()
    result.check_stdout('42')

def test_profiling_optimizer(shell):
    test = (
        ShellTest(shell)
        .statement("PRAGMA enable_profiling=query_tree_optimizer;")
        .statement("SELECT 42;")
    )
    result = test.run()
    result.check_stderr('Optimizer')
    result.check_stdout('42')

def test_profiling_optimizer_detailed(shell):
    test = (
        ShellTest(shell)
        .statement("PRAGMA enable_profiling;")
        .statement("PRAGMA profiling_mode=detailed;")
        .statement("SELECT 42;")
    )
    result = test.run()
    result.check_stderr('Optimizer')
    result.check_stdout('42')

def test_profiling_optimizer_json(shell):
    test = (
        ShellTest(shell)
        .statement("PRAGMA enable_profiling=json;")
        .statement("PRAGMA profiling_mode=detailed;")
        .statement("SELECT 42;")
    )
    result = test.run()
    result.check_stderr('optimizer')
    result.check_stdout('42')


# Original comment: this fails because db_config is missing
@pytest.mark.skip(reason="db_config is not supported (yet?)")
def test_eqp(shell):
    test = (
        ShellTest(shell)
        .statement(".eqp full")
        .statement("SELECT 42;")
    )
    result = test.run()
    result.check_stdout('DUMMY_SCAN')

def test_clone(shell, random_filepath):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE a (I INTEGER)")
        .statement("INSERT INTO a VALUES (42)")
        .statement(f".clone {random_filepath.as_posix()}")
    )
    result = test.run()
    result.check_stderr('unknown command or invalid arguments')


def test_databases(shell):
    test = (
        ShellTest(shell)
        .statement(".databases")
    )
    result = test.run()
    result.check_stdout('memory')


def test_dump_create(shell):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE a (i INTEGER);")
        .statement(".changes off")
        .statement("INSERT INTO a VALUES (42);")
        .statement(".dump")
    )
    result = test.run()
    result.check_stdout('CREATE TABLE a(i INTEGER)')
    result.check_stdout('COMMIT')

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
    result = test.run()
    result.check_stdout('CREATE TABLE a(i INTEGER)')

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
    result = test.run()
    result.check_stdout('CREATE TABLE a(d DATE, k FLOAT, t TIMESTAMP);')

def test_invalid_csv(shell, tmp_path):
    file = tmp_path / 'nonsencsv.csv'
    with open(file, 'wb+') as f:
        f.write(b'\xFF\n')
    test = (
        ShellTest(shell)
        .statement(".nullvalue NULL")
        .statement("CREATE TABLE test(i INTEGER);")
        .statement(f".import {file.as_posix()} test")
        .statement("SELECT * FROM test;")
    )
    result = test.run()
    result.check_stdout('NULL')

def test_mode_latex(shell):
    test = (
        ShellTest(shell)
        .statement(".mode latex")
        .statement("CREATE TABLE a (I INTEGER);")
        .statement(".changes off")
        .statement("INSERT INTO a VALUES (42);")
        .statement("SELECT * FROM a;")
    )
    result = test.run()
    result.check_stdout('\\begin{tabular}')

def test_mode_trash(shell):
    test = (
        ShellTest(shell)
        .statement(".mode trash")
        .statement("select 1")
    )
    result = test.run()
    result.check_stdout('')

@pytest.mark.skip(reason="Broken test, ported directly, was commented out")
def test_dump_blobs(shell):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE a (b BLOB);")
        .statement(".changes off")
        .statement("INSERT INTO a VALUES (DATE '1992-01-01', 0.3, NOW());")
        .statement(".dump")
    )
    result = test.run()
    result.check_stdout('COMMIT')

def test_sqlite_comments(shell):
    # Using /* <comment> */
    test = (
        ShellTest(shell)
        .statement("""/*
;
*/""")
        .statement("select 42")
    )
    result = test.run()
    result.check_stdout('42')

    # Using -- <comment>
    test = (
        ShellTest(shell)
        .statement("""-- this is a comment ;
select 42;
""")
    )
    result = test.run()
    result.check_stdout('42')

    # More extreme: -- <comment>
    test = (
        ShellTest(shell)
        .statement("""--;;;;;;
select 42;
""")
    )
    result = test.run()
    result.check_stdout('42')

    # More extreme: /* <comment> */
    test = (
        ShellTest(shell)
        .statement('/* ;;;;;; */ select 42;')
    )
    result = test.run()
    result.check_stdout('42')

def test_sqlite_udfs_error(shell):
    test = (
        ShellTest(shell)
        .statement("SELECT writefile()")
    )
    result = test.run()
    result.check_stderr('wrong number of arguments to function writefile')

    test = (
        ShellTest(shell)
        .statement("SELECT writefile('hello')")
    )
    result = test.run()
    result.check_stderr('wrong number of arguments to function writefile')

def test_sqlite_udfs_correct(shell, random_filepath):
    import os
    test = (
        ShellTest(shell)
        .statement(f"SELECT writefile('{random_filepath.as_posix()}', 'hello');")
    )
    result = test.run()
    if not os.path.exists(random_filepath):
        raise Exception(f"Failed to write file {random_filepath.as_posix()}")
    with open(random_filepath, 'r') as f:
        result.stdout = f.read()
    result.check_stdout('hello')

def test_lsmode(shell):
    test = (
        ShellTest(shell)
        .statement("SELECT lsmode(1) AS lsmode;")
    )
    result = test.run()
    result.check_stdout('lsmode')

def test_duckbox(shell):
    test = (
        ShellTest(shell)
        .statement(".mode duckbox")
        .statement("select 42 limit 0;")
    )
    result = test.run()
    result.check_stdout('0 rows')

# Original comment: #5411 - with maxrows=2, we still display all 4 rows (hiding them would take up more space)
def test_maxrows(shell):
    test = (
        ShellTest(shell)
        .statement(".maxrows 2")
        .statement("select * from range(4);")
    )
    result = test.run()
    result.check_stdout('1')

def test_maxrows_outfile(shell, random_filepath):
    file = random_filepath
    test = (
        ShellTest(shell)
        .statement(".maxrows 2")
        .statement(f".output {file.as_posix()}")
        .statement("SELECT * FROM range(100);")
    )
    result = test.run()
    result.stdout = open(file, 'rb').read().decode('utf8')
    result.check_stdout('50')

def test_columns_to_file(shell, random_filepath):
    columns = ', '.join([str(x) for x in range(100)])
    test = (
        ShellTest(shell)
        .statement(f".output {random_filepath.as_posix()}")
        .statement(f"SELECT {columns}")
    )
    result = test.run()
    result.stdout = open(random_filepath, 'rb').read().decode('utf8')
    result.check_stdout('99')

def test_columnar_mode(shell):
    test = (
        ShellTest(shell)
        .statement(".col")
        .statement("select * from range(4);")
    )
    result = test.run()
    result.check_stdout('Row 1')

def test_columnar_mode_constant(shell):
    columns = ','.join(["'MyValue" + str(x) + "'" for x in range(100)])
    test = (
        ShellTest(shell)
        .statement(".col")
        .statement(f"select {columns};")
    )
    result = test.run()
    result.check_stdout('MyValue50')

    test = (
        ShellTest(shell)
        .statement(".col")
        .statement(f"select {columns} from range(1000);")
    )
    result = test.run()
    result.check_stdout('100 columns')

def test_nullbyte_rendering(shell):
    test = (
        ShellTest(shell)
        .statement("select varchar from test_all_types();")
    )
    result = test.run()
    result.check_stdout('goo\\0se')

def test_nullbyte_error_rendering(shell):
    test = (
        ShellTest(shell)
        .statement("select chr(0)::int")
    )
    result = test.run()
    result.check_stderr('INT32')

@pytest.mark.parametrize("stmt", [
	"select sha3(NULL);"
])
def test_sqlite_udf_null(shell, stmt):
    test = (
        ShellTest(shell)
        .statement(stmt)
    )
    result = test.run()
    result.check_stdout('NULL')

def test_sqlite_udf_sha3_int(shell):
    test = (
        ShellTest(shell)
        .statement("select sha3(256)")
    )
    result = test.run()
    result.check_stdout('A7')

def test_sqlite_udf_sha3_non_inlined_string(shell):
    test = (
        ShellTest(shell)
        .statement("select sha3('hello world this is a long string');")
    )
    result = test.run()
    result.check_stdout('D4')

# fmt: on
