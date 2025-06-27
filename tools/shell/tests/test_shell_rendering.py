# fmt: off

import pytest
import subprocess
import sys
from typing import List
from conftest import ShellTest
import os
from pathlib import Path


def test_left_align(shell):
    test = (
        ShellTest(shell)
        .statement(".mode box")
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

def test_markdown(shell):
    test = (
        ShellTest(shell)
        .statement(".mode markdown")
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
