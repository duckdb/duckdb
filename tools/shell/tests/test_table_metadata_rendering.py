# fmt: off

import pytest
import subprocess
import sys
from typing import List
from conftest import ShellTest
import os
from pathlib import Path


def test_long_table_name(shell):
    test = (
        ShellTest(shell)
        .statement(".maxwidth 80")
        .statement(f'create table "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"(i int);')
        .statement('.tables')
    )

    result = test.run()
    result.check_stdout("…")

def test_long_column_name(shell):
    test = (
        ShellTest(shell)
        .statement(".maxwidth 80")
        .statement(f'describe select 42 as "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz";')
    )

    result = test.run()
    result.check_stdout("…")

def test_long_type_name(shell):
    test = (
        ShellTest(shell)
        .statement(".maxwidth 80")
        .statement('describe select {"abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz": 42} as a;')
    )

    result = test.run()
    result.check_stdout("…")

def test_long_type_and_column_name(shell):
    test = (
        ShellTest(shell)
        .statement(".maxwidth 80")
        .statement('describe select {"abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz": 42} as "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz";')
    )

    result = test.run()
    result.check_stdout("…")
