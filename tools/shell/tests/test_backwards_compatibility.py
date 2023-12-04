# fmt: off

import pytest
import subprocess
import sys
from typing import List
from conftest import ShellTest
import os

def test_version_dev(shell):
    test = (
        ShellTest(shell)
        .statement(".open test/storage/bc/db_dev.db")
    )
    result = test.run()
    result.check_stderr("older development version")

def test_version_0_3_1(shell):
    test = (
        ShellTest(shell)
        .statement(".open test/storage/bc/db_031.db")
    )
    result = test.run()
    result.check_stderr("v0.3.1")

def test_version_0_3_2(shell):
    test = (
        ShellTest(shell)
        .statement(".open test/storage/bc/db_032.db")
    )
    result = test.run()
    result.check_stderr("v0.3.2")

def test_version_0_4(shell):
    test = (
        ShellTest(shell)
        .statement(".open test/storage/bc/db_04.db")
    )
    result = test.run()
    result.check_stderr("v0.4.0")

def test_version_0_5_1(shell):
    test = (
        ShellTest(shell)
        .statement(".open test/storage/bc/db_051.db")
    )
    result = test.run()
    result.check_stderr("v0.5.1")

def test_version_0_6_0(shell):
    test = (
        ShellTest(shell)
        .statement(".open test/storage/bc/db_060.db")
    )
    result = test.run()
    result.check_stderr("v0.6.0")

# fmt: on
