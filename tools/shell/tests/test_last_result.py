# fmt: off

import pytest
import subprocess
import sys
import os
from typing import List
from conftest import ShellTest

# test that we can query the last result using _
def test_last_result(shell):
    test = (
        ShellTest(shell)
        .statement("SELECT 42 a")
        .statement('SELECT a + 100 FROM _')
    )
    result = test.run()
    result.check_stdout("142")

# no result available
def test_no_last_result(shell):
    test = (
        ShellTest(shell)
        .statement('FROM _')
    )
    result = test.run()
    result.check_stderr("no result available")

def test_stacking_last_result(shell):
    test = (
        ShellTest(shell)
        .statement("SELECT 42 a")
        .statement('SELECT a * 10 a FROM _')
        .statement('SELECT a * 10 a FROM _')
        .statement('SELECT a * 10 a FROM _')
        .statement('SELECT a * 10 a FROM _')
        .statement('SELECT a * 10 a FROM _')
    )
    result = test.run()
    result.check_stdout("4200000")

# errors do not overwrite last results
def test_last_result_error(shell):
    test = (
        ShellTest(shell)
        .statement("SELECT 42 a")
        .statement('select eek')
        .statement('SELECT a * 10 a FROM _')
    )
    result = test.run()
    assert '420' in result.stdout


# fmt: on
