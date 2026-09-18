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

def test_query_last_result(shell):
    test = (
        ShellTest(shell)
        .statement("SELECT 42 a")
        .statement('SELECT a * 10 a FROM _')
        .statement("FROM query('SELECT a * 10 a FROM _')")
        .statement('SELECT a * 10 a FROM _')
        .statement("FROM query('SELECT a * 10 a FROM _')")
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

def test_last_result_common_subplan_column_pruning(shell):
    common_subplan_query = """
        SELECT sum(a)
        FROM (
            SELECT retained.a
            FROM _ AS retained
            JOIN range(100) generated ON retained.a = generated.range
        )
        UNION ALL
        SELECT sum(b)
        FROM (
            SELECT retained.b
            FROM _ AS retained
            JOIN range(100) generated ON retained.a = generated.range
        )
    """
    retained_result = """
        SELECT i::BIGINT AS a, (i * 10)::BIGINT AS b
        FROM range(100) t(i)
    """
    test = (
        ShellTest(shell)
        .statement(".maxrows 2")
        .statement("SET profiling_renderer_settings = MAP {'operator_casing': 'upper'}")
        .statement("PRAGMA explain_output='optimized_only'")
        .statement(retained_result)
        .statement("EXPLAIN " + common_subplan_query)
        .statement(retained_result)
        .statement(common_subplan_query)
    )
    result = test.run()
    result.check_stdout("__common_subplan_1")
    assert result.stdout.count("CTE_SCAN") == 2
    assert "4950" in result.stdout
    assert "49500" in result.stdout


# fmt: on
