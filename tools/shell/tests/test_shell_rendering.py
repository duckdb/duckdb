# fmt: off

import pytest
import subprocess
import sys
from typing import List
from conftest import ShellTest
import os
from pathlib import Path


@pytest.mark.parametrize("dot_command", [
    ".mode box",
    ""
])
def test_left_align(shell, dot_command):
    args = ['-box'] if len(dot_command) == 0 else []
    test = (
        ShellTest(shell, args)
        .statement(dot_command)
        .statement(".width 5")
        .statement(f'select 100 AS r')
    )

    result = test.run()
    result.check_stdout("│ 100   │")

def test_right_align(shell):
    test = (
        ShellTest(shell)
        .statement(".mode box")
        .statement(".width -5")
        .statement(f'select 100 AS r')
    )

    result = test.run()
    result.check_stdout("│   100 │")

@pytest.mark.parametrize("dot_command", [
    ".mode markdown",
    ""
])
def test_markdown(shell, dot_command):
    args = ['-markdown'] if len(dot_command) == 0 else []
    test = (
        ShellTest(shell, args)
        .statement(dot_command)
        .statement("select 42 a, 'hello' str")
    )

    result = test.run()
    result.check_stdout("| a  |  str  |")

def test_mode_insert_table(shell):
    test = (
        ShellTest(shell)
        .statement(".mode box mytable")
    )

    result = test.run()
    result.check_stderr("TABLE argument can only be used with .mode insert")

def test_mode_box_newline(shell):
    test = (
        ShellTest(shell)
        .statement(".mode box")
        .statement("select '\n' c;")
    )

    result = test.run()
    result.check_stdout("\\n")

def test_mode_markdown_pipe(shell):
    test = (
        ShellTest(shell)
        .statement(".mode markdown")
        .statement("select 'val || other_val' c;")
    )

    result = test.run()
    result.check_stdout("\\|\\|")

def test_mode_json_null(shell):
    test = (
        ShellTest(shell)
        .statement(".mode json")
        .statement("select NULL as my_val;")
    )

    result = test.run()
    result.check_stdout("null")
