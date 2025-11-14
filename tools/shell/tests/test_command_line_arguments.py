# fmt: off

import pytest
import subprocess
import sys
from typing import List
from conftest import ShellTest
import os
from pathlib import Path

def test_headers(shell):
    test = (
        ShellTest(shell)
        .statement("SELECT 42 as wilbur")
        .add_argument("-csv", "-header")
    )
    result = test.run()
    result.check_stdout("wilbur")

def test_no_headers(shell):
    test = (
        ShellTest(shell)
        .statement("SELECT 42 as wilbur")
        .add_argument("-csv", "-noheader")
    )
    result = test.run()
    result.check_not_exist("wilbur")
def test_storage_version_latest(shell):
    test = (
        ShellTest(shell)
        .statement("SELECT 42")
        .add_argument("-storage_version", "latest")
    )
    result = test.run()
    result.check_stdout("42")

def test_command(shell):
    test = (
        ShellTest(shell)
        .add_argument("-c", "SELECT 42")
    )
    result = test.run()
    result.check_stdout("42")

def test_version(shell):
    test = (
        ShellTest(shell)
        .add_argument("-version")
    )
    result = test.run()
    result.check_stdout("v")

def test_csv_options(shell):
    test = (
        ShellTest(shell)
        .statement("SELECT 42 a, NULL b")
        .add_argument("-csv", "-nullvalue", "MYNULL", "-separator", "MYSEP")
    )
    result = test.run()
    result.check_stdout("42MYSEPMYNULL")

def test_echo(shell):
    test = (
        ShellTest(shell, ['-echo'])
        .statement("SELECT 42;")
    )
    result = test.run()
    result.check_stdout("SELECT 42")

def test_storage_version_error(shell):
    test = (
        ShellTest(shell)
        .statement("SELECT 42")
        .add_argument("-storage-version", "XXX")
    )
    result = test.run()
    result.check_stderr("XXX")


# fmt: on
