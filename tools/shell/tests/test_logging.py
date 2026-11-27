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
        .statement("CALL enable_logging('QueryLog', sink='stdout')")
        .statement('SELECT 1 as a')
    )
    result = test.run()

    newline = "\r\n" if os.name == "nt" else "\n"
    result.check_stdout(f"QueryLog\tINFO\tSELECT 1 as a;{newline}┌───────┐")


def test_logging_custom_delim(shell):
    test = (
        ShellTest(shell)
        .statement("CALL enable_logging('QueryLog', sink='stdout', sink_config={'delim':','})")
        .statement('SELECT 1 as a')
    )
    result = test.run()
    newline = "\r\n" if os.name == "nt" else "\n"
    result.check_stdout(f"QueryLog,INFO,SELECT 1 as a;{newline}┌───────┐")

def test_logging_shell_pretty(shell):
    # INFO/TRACE logs render as a compact single line "LEVEL:type   <elapsed>  <message>" via
    # the shell log sink. '.highlight off' makes the output deterministic (no ANSI). The elapsed
    # value is measured from CLI launch, so assert on the prefix + message, not the timing.
    test = (
        ShellTest(shell)
        .statement(".highlight off")
        .statement("CALL enable_logging('QueryLog')")
        .statement('SELECT 1 as a')
    )
    result = test.run()
    lines = result.stdout.splitlines()
    assert any(ln.startswith("INFO:QueryLog") and ln.endswith("SELECT 1 as a;") for ln in lines)


def test_logging_shell_pretty_multiline(shell):
    # A multi-line statement must be sanitized onto a single log line - no prefix-less
    # continuation lines. Regression test for the multi-line message handling.
    test = (
        ShellTest(shell)
        .statement(".highlight off")
        .statement("CALL enable_logging('QueryLog')")
        .statement("SELECT\n  1,\n  2")
    )
    result = test.run()
    lines = result.stdout.splitlines()
    assert any(ln.startswith("INFO:QueryLog") and ln.endswith("SELECT 1, 2;") for ln in lines)


def test_logging_warning_dedup(shell):
    # Repeated identical WARNINGs are de-duplicated: printed only once.
    test = (
        ShellTest(shell)
        .statement(".highlight off")
        .statement("SELECT write_log('dup', level := 'WARNING')")
        .statement("SELECT write_log('dup', level := 'WARNING')")
    )
    result = test.run()
    # The message 'dup' appears on its own line only once (result-box headers also echo the SQL,
    # so filter to standalone lines).
    lines = [ln.strip() for ln in result.stdout.splitlines()]
    assert lines.count("dup") == 1


def test_logging_error_no_dedup(shell):
    # Repeated identical ERRORs are NOT de-duplicated: every occurrence is printed.
    test = (
        ShellTest(shell)
        .statement(".highlight off")
        .statement("SELECT write_log('dup', level := 'ERROR')")
        .statement("SELECT write_log('dup', level := 'ERROR')")
    )
    result = test.run()
    lines = [ln.strip() for ln in result.stdout.splitlines()]
    assert lines.count("dup") == 2


# By default stdoutlogging has buffer size of 1, but we can increase it if we want. We use `only_flush_on_full_buffer` to ensure we can test this
def test_logging_buffering(shell):
    test = (
        ShellTest(shell)
        .statement("CALL enable_logging('QueryLog', sink='stdout', sink_buffer_size=1000, sink_config={'only_flush_on_full_buffer': true})")
        .statement('SELECT 1 as a')
        .statement('SELECT 2 as b')
    )
    result = test.run()
    result.check_not_exist("QueryLog")


def test_logs_are_queryable_in_a_memory_sink_in_the_cli(shell):
    # the CLI's default log sink prints the warnings, a memory sink of its own keeps them queryable
    test = (
        ShellTest(shell)
        .statement("CALL enable_log_sink('my_logs', type := 'memory')")
        .statement("SELECT write_log('cli warning', level := 'WARNING')")
        .statement("SELECT 'stored-' || count(*) AS stored FROM duckdb_logs('my_logs') WHERE message = 'cli warning'")
    )
    result = test.run()
    result.check_stdout("stored-1")


def test_default_sink_cannot_be_read_in_the_cli(shell):
    test = ShellTest(shell).statement("FROM duckdb_logs")
    result = test.run()
    result.check_stderr("Log sink 'default' is a 'shell_log_sink' log sink, which cannot be read")
    result.check_stderr("CALL enable_log_sink('my_logs', type := 'memory')")


def test_logging_storage_shows_the_old_shell_name(shell):
    # the deprecated setting shows the name the CLI's sink had in 2.0.0
    test = ShellTest(shell).statement("SELECT 'storage-' || current_setting('logging_storage') AS storage")
    result = test.run()
    result.check_stdout("storage-shell_log_storage")


def test_stdout_sink_in_the_cli_warns_about_printing_twice(shell):
    # a stdout sink next to the CLI's default log sink prints the log entries twice
    test = ShellTest(shell).statement("CALL enable_log_sink('stdout')")
    result = test.run()
    result.check_stdout("both print the log entries to the console")
    result.check_stdout("To print them once, run CALL disable_logging().")

    # a stdout default log sink replaces the CLI's sink, which stops printing the warnings
    test = ShellTest(shell).statement("CALL enable_logging(sink='stdout')")
    result = test.run()
    result.check_not_exist("both print the log entries to the console")
    result.check_stdout("CALL enable_log_sink('shell_log_sink', level := 'warning')")

    # a shell sink of its own next to a stdout default log sink prints them twice as well
    test = (
        ShellTest(shell)
        .statement("CALL enable_logging(sink='memory')")
        .statement("CALL enable_log_sink('shell_log_sink', level := 'warning')")
        .statement("CALL enable_logging(sink='stdout')")
    )
    result = test.run()
    result.check_stdout("To print them once, run CALL disable_log_sink('shell_log_sink').")

    # without the CLI's sink, stdout is the only sink that prints
    test = (
        ShellTest(shell)
        .statement("CALL disable_logging()")
        .statement("CALL enable_log_sink('stdout')")
    )
    result = test.run()
    result.check_not_exist("both print the log entries to the console")

    # the queries are not the warnings that the CLI's sink prints
    test = ShellTest(shell).statement("CALL enable_log_sink('stdout', log_types := ['QueryLog'])")
    result = test.run()
    result.check_not_exist("log entries to the console")

    # until the stdout sink takes every log type
    test = (
        ShellTest(shell)
        .statement("CALL enable_log_sink('stdout', log_types := ['QueryLog'])")
        .statement("CALL enable_log_sink('stdout', log_types := [])")
    )
    result = test.run()
    result.check_stdout("both print the log entries to the console")

    # a stdout sink for the errors prints them twice, but not the warnings, which the CLI's sink still has to print
    test = ShellTest(shell).statement("CALL enable_log_sink('stdout', level := 'error')")
    result = test.run()
    result.check_stdout("both print some of the log entries to the console")
    result.check_stdout("To print them once, give the 'stdout' log sink log types that the default log sink does not print.")

# fmt: on
