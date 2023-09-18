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
