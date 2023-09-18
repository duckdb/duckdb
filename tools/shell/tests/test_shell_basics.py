import pytest
import subprocess
import sys
from typing import List
from conftest import ShellTest


def assert_expected_res(out, expected, status, err):
    if expected not in out:
        print("Exit code:", status)
        print("Captured stderr:", err)
        print("Actual result:", out)
        assert False
    assert status == 0
    assert True


def assert_expected_err(out, expected, status, err):
    if expected not in err:
        print("Exit code:", status)
        print("Captured stdout:", out)
        print("Actual error:", err)
        assert False
    assert True


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
    test = ShellTest(shell).statement(
        """select 'hello
world' as a"""
    )

    out, err, status = test.run()
    assert_expected_res(out, "hello\\nworld", status, err)


def test_newline_in_column_name(shell):
    test = ShellTest(shell).statement(
        """select 42 as "hello
world" """
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

    test = ShellTest(shell).statement(f".cd {tmp_path}").statement(f".cd {current_dir}")
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
    test = ShellTest(shell).statement(".echo on").statement("SELECT 42;")
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
    test = ShellTest(shell).statement(".headers on").statement("SELECT 42 as wilbur")
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
    test = ShellTest(shell).statement(".help")
    out, err, status = test.run()
    assert_expected_res(out, "Show help text for PATTERN", status, err)


def test_load_error(shell, random_filepath):
    test = ShellTest(shell).statement(f".load {random_filepath}")
    out, err, status = test.run()
    assert_expected_err(out, "Error", status, err)


def test_streaming_error(shell):
    test = ShellTest(shell).statement(
        "SELECT x::INT FROM (SELECT x::VARCHAR x FROM range(10) tbl(x) UNION ALL SELECT 'hello' x) tbl(x);"
    )
    out, err, status = test.run()
    assert_expected_err(out, "Could not convert string", status, err)


@pytest.mark.parametrize(
    "stmt", ["explain select sum(i) from range(1000) tbl(i)", "explain analyze select sum(i) from range(1000) tbl(i)"]
)
def test_explain(shell, stmt):
    test = ShellTest(shell).statement(stmt)
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
    test = ShellTest(shell).statement("CREATE TABLE table1 (mylittlecolumn INTEGER);").statement("show table1;")
    out, err, status = test.run()
    assert_expected_res(out, "mylittlecolumn", status, err)


def test_call_display(shell):
    test = ShellTest(shell).statement("CALL range(4);")
    out, err, status = test.run()
    assert_expected_res(out, "3", status, err)


def test_execute_display(shell):
    test = ShellTest(shell).statement("PREPARE v1 AS SELECT ?::INT;").statement("EXECUTE v1(42);")
    out, err, status = test.run()
    assert_expected_res(out, "42", status, err)


# this should be fixed
def test_selftest(shell):
    test = ShellTest(shell).statement(".selftest")
    out, err, status = test.run()
    assert_expected_err(out, "sqlite3_table_column_metadata", status, err)


@pytest.mark.parametrize('generated_file', ["select 42"], indirect=True)
def test_read(shell, generated_file):
    test = ShellTest(shell).statement(f".read {generated_file}")
    out, err, status = test.run()
    assert_expected_res(out, "42", status, err)
