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

# alias on _ is reachable from a qualified column reference
def test_last_result_alias(shell):
    test = (
        ShellTest(shell)
        .statement("SELECT 'a' AS x")
        .statement("SELECT d.x FROM _ AS d")
    )
    result = test.run()
    result.check_stdout("a")

# self-join over _ via two aliases (regression for #22841)
def test_last_result_alias_self_join(shell):
    test = (
        ShellTest(shell)
        .statement("SELECT 'a' AS x")
        .statement("SELECT d1.x, d2.x FROM _ AS d1, _ AS d2")
    )
    result = test.run()
    assert 'd1' not in result.stderr
    result.check_stdout("a")


# fmt: on
