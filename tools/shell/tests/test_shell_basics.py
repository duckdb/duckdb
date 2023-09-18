import pytest
import subprocess
import sys
from typing import List
from conftest import ShellTest


def assert_expected_res(out, expected, status, err):
    assert status == 0
    if expected not in out:
        print("Exit code:", status)
        print("Captured stderr:", err)
        print("Actual result:", out)
        assert False
    assert status == 0
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
def test_shell_import(shell, generated_file):
    test = (
        ShellTest(shell)
        .statement(".mode csv")
        .statement(f'.import "{generated_file}" test_table')
        .statement("select * FROM test_table")
    )

    out, err, status = test.run()
    assert_expected_res(out, "col_1,col_2\n1,2\n10,20", status, err)
