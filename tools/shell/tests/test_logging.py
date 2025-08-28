# fmt: off

import pytest
import subprocess
import sys
import os
from typing import List
from conftest import ShellTest

def test_logging(shell):
    test = (
        ShellTest(shell)
        .statement("CALL enable_logging('QueryLog', storage='stdout')")
        .statement('SELECT 1 as a')
    )
    result = test.run()

    newline = "\r\n" if os.name == "nt" else "\n"
    result.check_stdout(f"QueryLog\tINFO\tSELECT 1 as a;{newline}┌───────┐")


def test_logging_custom_delim(shell):
    test = (
        ShellTest(shell)
        .statement("CALL enable_logging('QueryLog', storage='stdout', storage_config={'delim':','})")
        .statement('SELECT 1 as a')
    )
    result = test.run()
    newline = "\r\n" if os.name == "nt" else "\n"
    result.check_stdout(f"QueryLog,INFO,SELECT 1 as a;{newline}┌───────┐")

# By default stdoutlogging has buffer size of 1, but we can increase it if we want. We use `only_flush_on_full_buffer` to ensure we can test this
def test_logging_buffering(shell):
    test = (
        ShellTest(shell)
        .statement("CALL enable_logging('QueryLog', storage='stdout', storage_buffer_size=1000, storage_config={'only_flush_on_full_buffer': true})")
        .statement('SELECT 1 as a')
        .statement('SELECT 2 as b')
    )
    result = test.run()
    result.check_not_exist("QueryLog")

# fmt: on
