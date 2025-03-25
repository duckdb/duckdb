# fmt: off

import pytest
import subprocess
import sys
from typing import List
from conftest import ShellTest


def test_profiling_json(shell):
    test = (
        ShellTest(shell)
        .statement("PRAGMA enable_profiling=json;")
        .statement('CREATE TABLE "foo"("hello world" INT);')
        .statement("""SELECT "hello world", '\r\t\n\b\f\\' FROM "foo";""")
    )
    result = test.run()
    result.check_stderr(r'"hello world"')
    # This is incorrectly split but that's impossible to do correctly currently.
    result.check_stderr(r''''\r\t"''')
    result.check_stderr(r""""\b\f\\'""")

# fmt: on
