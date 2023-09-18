# fmt: off

import pytest
import subprocess
import sys
from typing import List
from conftest import ShellTest, assert_expected_res, assert_expected_err
import os

def test_version_dev(shell):
    test = (
        ShellTest(shell)
        .statement(".open test/storage/bc/db_dev.db")
    )
    out, err, status = test.run()
    assert_expected_err(out, "older development version", status, err)

def test_version_0_3_1(shell):
    test = (
        ShellTest(shell)
        .statement(".open test/storage/bc/db_031.db")
    )
    out, err, status = test.run()
    assert_expected_err(out, "v0.3.1", status, err)

def test_version_0_3_2(shell):
    test = (
        ShellTest(shell)
        .statement(".open test/storage/bc/db_032.db")
    )
    out, err, status = test.run()
    assert_expected_err(out, "v0.3.2", status, err)

def test_version_0_4(shell):
    test = (
        ShellTest(shell)
        .statement(".open test/storage/bc/db_04.db")
    )
    out, err, status = test.run()
    assert_expected_err(out, "v0.4.0", status, err)

def test_version_0_5_1(shell):
    test = (
        ShellTest(shell)
        .statement(".open test/storage/bc/db_051.db")
    )
    out, err, status = test.run()
    assert_expected_err(out, "v0.5.1", status, err)

def test_version_0_6_0(shell):
    test = (
        ShellTest(shell)
        .statement(".open test/storage/bc/db_060.db")
    )
    out, err, status = test.run()
    assert_expected_err(out, "v0.6.0", status, err)

# fmt: on
