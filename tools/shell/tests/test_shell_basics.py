import pytest
import subprocess
import sys
from typing import List
from conftest import ShellTest

# fmt: off
@pytest.mark.parametrize(
    ["command", "expected_result"],
    [
        (
            "select 'asdf' as a;",
            "asdf"
        ),
        (
            "select * from range(10000);",
            "9999"
        ),
        (
""".mode csv
.headers off
.sep |
CREATE TABLE t0(c0 INT);
PRAGMA table_info('t0');
""",
            "0|c0|INTEGER|false||false"
        )
    ]
)
# fmt: on
def test_shell_basic(shell, command, expected_result):
    test = ShellTest(shell)
    out, _, status = test.run(command)
    assert status == 0
    if expected_result not in out:
        print(out)
        assert False
    assert True


@pytest.mark.parametrize(
    ["command", "expected_result"],
    [
        (
"""
.mode csv
.import {input_file} test_table
SELECT * FROM test_table;
""",
"col_1,col_2\n1,2\n10,20"
        )
    ],
)
@pytest.mark.parametrize('generated_file', ["col_1,col_2\n1,2\n10,20"], indirect=True)
def test_shell_import(shell, command, expected_result, generated_file):
    command = command.format(input_file=generated_file).strip("\n ").replace('\t', '')
    test = ShellTest(shell)
    out, err, status = test.run(command)
    if expected_result not in out:
        print("Exit code:", status)
        print("Captured stderr:", err)
        print("Command:")
        print(command)
        print("Actual result:", out)
        assert False
    assert status == 0
    assert True
