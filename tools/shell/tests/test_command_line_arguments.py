# fmt: off

import pytest
import subprocess
import sys
from typing import List
from conftest import ShellTest, assert_loaded
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

# `-serve`/`-connect` only expand their command template - quack resolves the secret, its token and the
# endpoint on its own. The probes print the expanded command instead of running it.
SERVE_PROBE = '.serve_command ".print serve -> quack_serve({serve_secret|})"'
CONNECT_PROBE = '.connect_command ".print connect -> CONNECT quack:{connect_secret|}"'

def test_serve_defaults(shell):
    result = ShellTest(shell).add_argument("-cmd", SERVE_PROBE, "-serve", "-no-stdin").run()
    result.check_stdout("serve -> quack_serve()")

def test_connect_defaults(shell):
    result = ShellTest(shell).add_argument("-cmd", CONNECT_PROBE, "-connect", "-no-stdin").run()
    result.check_stdout("connect -> CONNECT quack:")

def test_serve_secret(shell):
    result = ShellTest(shell).add_argument("-cmd", SERVE_PROBE, "-serve", "-secret", "prod", "-no-stdin").run()
    result.check_stdout("serve -> quack_serve(secret='prod')")

def test_connect_secret(shell):
    result = ShellTest(shell).add_argument("-cmd", CONNECT_PROBE, "-connect", "-secret", "prod", "-no-stdin").run()
    result.check_stdout("connect -> CONNECT quack: (SECRET prod )")

def test_secret_name_is_escaped(shell):
    # the name ends up inside a SQL string literal, so a quote in it has to be doubled
    test = ShellTest(shell).add_argument(
        "-cmd", '.serve_command ".print [{serve_secret|}]"', "-serve", "-secret", "we'ird", "-no-stdin"
    )
    result = test.run()
    result.check_stdout("[secret='we''ird']")

def test_serve_missing_parameter(shell):
    test = (
        ShellTest(shell)
        .add_argument("-cmd", ".serve_command .print {unknown_parameter}", "-serve", "-no-stdin")
    )
    result = test.run()
    result.check_stderr("no value provided for parameter 'unknown_parameter'")

def test_default_secret_is_seeded_when_there_is_none(shell):
    # with no credentials at all a fallback secret is created, so -serve/-connect work out of the box
    assert_loaded(shell, "quack")
    test = ShellTest(shell).add_argument(
        "-cmd", ".connect_command \".print connected\"", "-connect", "-no-stdin", "-list", "-noheader",
    ).add_argument("-cmd", "SELECT name, type FROM duckdb_secrets()")
    result = test.run()
    result.check_stdout("__default_quack|quack")

def test_default_secret_is_not_seeded_over_an_existing_one(shell):
    # a secret of the user's own is left alone - seeding one would shadow it
    assert_loaded(shell, "quack")
    test = ShellTest(shell).add_argument(
        "-cmd", "LOAD quack; CREATE SECRET mine (TYPE quack, TOKEN 'mytoken', SCOPE 'quack:localhost:9494');",
        "-cmd", ".connect_command \".print connected\"", "-connect", "-no-stdin", "-list", "-noheader",
    ).add_argument("-cmd", "SELECT name FROM duckdb_secrets() ORDER BY name")
    result = test.run()
    result.check_stdout("mine")
    result.check_not_exist("__default_quack")

def test_serve_command_runs_against_quack(shell):
    # the built-in commands have to be valid quack SQL - a bad one would fail at parse/bind time
    assert_loaded(shell, "quack")
    result = ShellTest(shell).add_argument("-cmd", "LOAD quack;", "-connect", "-secret", "nope", "-no-stdin").run()
    result.check_stderr('Secret with name "nope" not found')


# fmt: on
