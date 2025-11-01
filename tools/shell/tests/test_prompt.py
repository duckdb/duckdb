# fmt: off

import pytest
import subprocess
import sys
from typing import List
from conftest import ShellTest


def test_prompt_unterminated_bracket(shell):
    test = (
        ShellTest(shell)
        .statement('.prompt {')
    )
    result = test.run()
    result.check_stderr("unterminated bracket")

def test_prompt_unterminated_escape(shell):
    test = (
        ShellTest(shell)
        .statement('.prompt \\')
    )
    result = test.run()
    result.check_stderr("unterminated")

def test_prompt_invalid_option(shell):
    test = (
        ShellTest(shell)
        .statement('.prompt "{yyy}"')
    )
    result = test.run()
    result.check_stderr("yyy")

def test_prompt_missing_query(shell):
    test = (
        ShellTest(shell)
        .statement('.prompt "{sql}"')
    )
    result = test.run()
    result.check_stderr("sql requires a parameter")

def test_prompt_invalid_color(shell):
    test = (
        ShellTest(shell)
        .statement('.prompt "{color:xxx}"')
    )
    result = test.run()
    result.check_stderr("xxx")

# fmt: on
