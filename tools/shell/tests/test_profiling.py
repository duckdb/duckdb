# fmt: off

import pytest
import subprocess
import sys
from typing import List
from conftest import ShellTest, assert_expected_res, assert_expected_err


def test_profiling_json(shell):
    test = (
        ShellTest(shell)
        .statement("PRAGMA enable_profiling=json;")
        .statement('CREATE TABLE "foo"("hello world" INT);')
        .statement("""SELECT "hello world", '\r\t\n\b\f\\' FROM "foo";""")
    )
    out, err, status = test.run()
    expected = """SELECT \\"hello world\\", '\\r\\t\\n\\b\\f\\\\' FROM \\"foo"""
    assert_expected_err(out, expected, status, err)

# fmt: on
