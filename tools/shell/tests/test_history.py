# fmt: off

import pytest
import subprocess
import sys
import os
from typing import List
from conftest import ShellTest

# the shell_history() table function returns the previously executed statements
def test_shell_history_function(shell):
    test = (
        ShellTest(shell)
        .statement("SELECT 42 AS a")
        .statement("SELECT 'hello' AS b")
        .statement("SELECT sql FROM shell_history() ORDER BY id")
    )
    result = test.run()
    result.check_stdout("SELECT 42 AS a")
    result.check_stdout("SELECT 'hello' AS b")

# the history is queryable - we can filter/aggregate over it
def test_shell_history_queryable(shell):
    test = (
        ShellTest(shell)
        .statement("SELECT 1")
        .statement("SELECT 2")
        .statement("SELECT count(*) AS c FROM shell_history()")
    )
    result = test.run()
    # 2 prior statements + the count query itself = 3 entries
    result.check_stdout("3")

# the id column is a monotonically increasing 1-based index
def test_shell_history_ids(shell):
    test = (
        ShellTest(shell)
        .statement("SELECT 10")
        .statement("SELECT min(id) AS lo, max(id) AS hi FROM shell_history()")
    )
    result = test.run()
    result.check_stdout("1")

# the .history dot command prints the command history
def test_history_command(shell):
    test = (
        ShellTest(shell)
        .statement("SELECT 42 AS a")
        .statement("SELECT 'hello' AS b")
        .statement(".history")
    )
    result = test.run()
    result.check_stdout("SELECT 42 AS a")
    result.check_stdout("SELECT 'hello' AS b")

# .history N only shows the last N entries (the .history command itself counts as an entry)
def test_history_command_limit(shell):
    test = (
        ShellTest(shell)
        .statement("SELECT 111")
        .statement("SELECT 222")
        .statement("SELECT 333")
        .statement(".history 2")
    )
    result = test.run()
    # the last two entries are "SELECT 333" and ".history 2" - "SELECT 333" should be shown
    result.check_stdout("SELECT 333")
    # earlier entries should not be shown
    result.check_not_exist("SELECT 111")

# multi-line statements are printed with continuation lines aligned under the SQL
def test_history_multiline_alignment(shell):
    test = (
        ShellTest(shell)
        .statement("SELECT\n2,\n3")
        .statement(".history")
    )
    result = test.run()
    # the prefix "    1  " is 7 characters wide - continuation lines are padded to match
    result.check_stdout("    1  SELECT\n       2,\n       3;")

# .history rejects too many arguments
def test_history_command_usage(shell):
    test = (
        ShellTest(shell)
        .statement(".history a b c")
    )
    result = test.run()
    result.check_stderr("Usage")

# fmt: on
