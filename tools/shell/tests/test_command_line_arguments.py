# fmt: off

import pytest
import subprocess
import sys
from typing import List
from conftest import ShellTest
import os
from pathlib import Path

def test_missing_arg(shell):
    test = (
        ShellTest(shell)
        .statement("SELECT 42")
        .add_argument("-xyz")
    )
    result = test.run()
    result.check_stderr("Unrecognized option")
    result.check_stderr("xyz")

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

SERVE_TEMPLATE = ".serve_command .print quack:{host|localhost}:{port|9494} token={token|quack}"
CONNECT_TEMPLATE = ".connect_command .print attach quack:{host|localhost}:{port|9494} token={token|quack}"

def test_serve_defaults(shell):
    test = (
        ShellTest(shell)
        .add_argument("-cmd", SERVE_TEMPLATE, "-serve", "-no-stdin")
    )
    result = test.run()
    result.check_stdout("quack:localhost:9494 token=quack")

def test_serve_parameters(shell):
    test = (
        ShellTest(shell)
        .add_argument("-cmd", SERVE_TEMPLATE, "-serve", "-host", "myhost", "-port", "1234", "-token", "mytoken", "-no-stdin")
    )
    result = test.run()
    result.check_stdout("quack:myhost:1234 token=mytoken")

def test_serve_parameters_before_command(shell):
    # parameters are set in the first pass - so they can be provided after the -serve flag
    test = (
        ShellTest(shell)
        .add_argument("-port", "1234", "-cmd", SERVE_TEMPLATE, "-serve", "-no-stdin")
    )
    result = test.run()
    result.check_stdout("quack:localhost:1234 token=quack")

def test_connect(shell):
    test = (
        ShellTest(shell)
        .add_argument("-cmd", CONNECT_TEMPLATE, "-connect", "-host", "myhost", "-no-stdin")
    )
    result = test.run()
    result.check_stdout("attach quack:myhost:9494 token=quack")

def test_serve_missing_parameter(shell):
    test = (
        ShellTest(shell)
        .add_argument("-cmd", ".serve_command .print {unknown_parameter}", "-serve", "-no-stdin")
    )
    result = test.run()
    result.check_stderr("no value provided for parameter 'unknown_parameter'")

def test_invalid_port(shell):
    test = (
        ShellTest(shell)
        .statement("SELECT 42")
        .add_argument("-port", "abc")
    )
    result = test.run()
    result.check_stderr("invalid argument (abc) for '-port'")


# fmt: on
