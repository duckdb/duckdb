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

def test_encryption_key_requires_file(shell):
    # encryption key should fail with in-memory database
    test = (
        ShellTest(shell)
        .add_argument("-encryption-key", "my_secret_key")
        .add_argument("-c", "SELECT 42")
    )
    result = test.run()
    result.check_stderr("requires a database file path")

def test_encryption_cipher_invalid(shell):
    # invalid cipher should fail
    test = (
        ShellTest(shell)
        .add_argument("-encryption-cipher", "INVALID")
        .add_argument("-c", "SELECT 42")
    )
    result = test.run()
    result.check_stderr("invalid encryption cipher")

def test_encryption_cipher_valid(shell):
    # valid cipher should not error on its own
    test = (
        ShellTest(shell)
        .add_argument("-encryption-cipher", "CTR")
        .add_argument("-c", "SELECT 42")
    )
    result = test.run()
    result.check_stdout("42")

def test_encryption_key_with_file(shell, tmp_path):
    # encryption with file database should work
    db_path = tmp_path / "encrypted.db"
    test = (
        ShellTest(shell)
        .add_argument(str(db_path))
        .add_argument("-encryption-key", "my_secret_key")
        .add_argument("-c", "CREATE TABLE t(i INTEGER); INSERT INTO t VALUES (42); SELECT * FROM t;")
    )
    result = test.run()
    result.check_stdout("42")

def test_encryption_reopen_with_key(shell, tmp_path):
    # create encrypted database
    db_path = tmp_path / "encrypted.db"
    test1 = (
        ShellTest(shell)
        .add_argument(str(db_path))
        .add_argument("-encryption-key", "my_secret_key")
        .add_argument("-c", "CREATE TABLE t(i INTEGER); INSERT INTO t VALUES (42);")
    )
    test1.run()
    # reopen with correct key
    test2 = (
        ShellTest(shell)
        .add_argument(str(db_path))
        .add_argument("-encryption-key", "my_secret_key")
        .add_argument("-c", "SELECT * FROM t;")
    )
    result = test2.run()
    result.check_stdout("42")

def test_encryption_wrong_key(shell, tmp_path):
    # create encrypted database
    db_path = tmp_path / "encrypted.db"
    test1 = (
        ShellTest(shell)
        .add_argument(str(db_path))
        .add_argument("-encryption-key", "my_secret_key")
        .add_argument("-c", "CREATE TABLE t(i INTEGER);")
    )
    test1.run()
    # reopen with wrong key
    test2 = (
        ShellTest(shell)
        .add_argument(str(db_path))
        .add_argument("-encryption-key", "wrong_key")
        .add_argument("-c", "SELECT 1;")
    )
    result = test2.run()
    result.check_stderr("Wrong encryption key")

def test_encryption_with_cipher(shell, tmp_path):
    # encryption with CTR cipher
    db_path = tmp_path / "encrypted_ctr.db"
    test = (
        ShellTest(shell)
        .add_argument(str(db_path))
        .add_argument("-encryption-key", "my_secret_key")
        .add_argument("-encryption-cipher", "CTR")
        .add_argument("-c", "CREATE TABLE t(i INTEGER); INSERT INTO t VALUES (99); SELECT * FROM t;")
    )
    result = test.run()
    result.check_stdout("99")


# fmt: on
