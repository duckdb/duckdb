# fmt: off

import pytest
import subprocess
import sys
from typing import List
from conftest import ShellTest
import os

lineitem_ddl = 'CREATE TABLE lineitem(l_orderkey BIGINT NOT NULL, l_partkey BIGINT NOT NULL, l_suppkey BIGINT NOT NULL, l_linenumber BIGINT NOT NULL, l_quantity DECIMAL(15,2) NOT NULL, l_extendedprice DECIMAL(15,2) NOT NULL, l_discount DECIMAL(15,2) NOT NULL, l_tax DECIMAL(15,2) NOT NULL, l_returnflag VARCHAR NOT NULL, l_linestatus VARCHAR NOT NULL, l_shipdate DATE NOT NULL, l_commitdate DATE NOT NULL, l_receiptdate DATE NOT NULL, l_shipinstruct VARCHAR NOT NULL, l_shipmode VARCHAR NOT NULL, l_comment VARCHAR NOT NULL);'

@pytest.mark.skipif(os.name == 'nt', reason="Windows highlighting does not use shell escapes")
def test_highlight_column_header(shell):
    test = (
        ShellTest(shell)
        .statement(".highlight_results on")
        .statement('select NULL AS r;')
    )
    result = test.run()
    result.check_stdout('\x1b[90mNULL\x1b[00m')
@pytest.mark.skipif(os.name == 'nt', reason="Windows highlighting does not use shell escapes")
def test_custom_highlight(shell):
    test = (
        ShellTest(shell)
        .statement(".highlight_results on")
        .statement(".highlight_colors column_name red bold")
        .statement(".highlight_colors column_type yellow")
        .statement(lineitem_ddl)
        .statement('select * from lineitem;')
    )
    result = test.run()
    result.check_stdout('\x1b[1m\x1b[31ml_comment\x1b[00m')
    result.check_stdout('\x1b[33mvarchar\x1b[00m')

def test_custom_highlight_error(shell):
    test = (
        ShellTest(shell)
        .statement(".highlight_colors column_nameXX red")
        .statement(".highlight_colors column_name redXX")
        .statement(".highlight_colors column_name red boldXX")
        .statement(".highlight_colors column_name red bold zz")
    )
    result = test.run()
    result.check_stderr("Unknown element 'column_nameXX'")
    result.check_stderr("Unknown highlighting color 'redXX'")
    result.check_stderr("Unknown intensity 'boldXX'")
    result.check_stderr("Usage")

@pytest.mark.skipif(os.name == 'nt', reason="Deprecated highlighting commands")
def test_deprecated_highlight_commands(shell):
    test = (
        ShellTest(shell)
        .statement(".keyword brightred")
        .statement(".comment brightred")
        .statement(".error brightred")
        .statement(".cont brightred")
        .statement(".cont_sel brightred")
        .statement("select 42;")
    )
    result = test.run()
    result.check_stdout("42")
    result.check_stderr("use .highlight_colors keyword brightred instead")
    result.check_stderr("use .highlight_colors comment brightred instead")
    result.check_stderr("use .highlight_colors error brightred instead")
    result.check_stderr("use .highlight_colors continuation brightred instead")
    result.check_stderr("use .highlight_colors continuation_selected brightred instead")
    assert "Unknown" not in result.stderr
    assert "render_color" not in result.stderr

@pytest.mark.skipif(os.name == 'nt', reason="Deprecated highlighting commands")
def test_deprecated_highlight_constant(shell):
    test = ShellTest(shell).statement(".constant brightred")
    result = test.run()
    result.check_stderr(".constant has been split into numeric_constant and string_constant")
    result.check_stderr(".highlight_colors numeric_constant brightred and .highlight_colors string_constant brightred")

def run_terminal(shell, arguments, environment, response=None):
    import fcntl
    import pty
    import select
    import struct
    import termios
    import time

    master, slave = pty.openpty()
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack('HHHH', 24, 80, 0, 0))
    start = time.monotonic()
    try:
        process = subprocess.Popen(
            [shell, '-no-init', *arguments],
            stdin=slave,
            stdout=slave,
            stderr=slave,
            env=environment,
        )
    finally:
        os.close(slave)
    output = bytearray()
    reply_at = None
    try:
        while time.monotonic() - start < 15:
            if select.select([master], [], [], 0.05)[0]:
                try:
                    chunk = os.read(master, 4096)
                except OSError:
                    break
                if not chunk:
                    break
                output.extend(chunk)
            if response and reply_at is None and b'\x1b]11;?' in output:
                reply_at = time.monotonic() + 0.8
            if reply_at and time.monotonic() >= reply_at:
                os.write(master, response)
                response = None
                reply_at = None
            if process.poll() is not None and not select.select([master], [], [], 0)[0]:
                break
        try:
            status = process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            pytest.fail('shell did not exit')
        return status, bytes(output), time.monotonic() - start
    finally:
        if process.poll() is None:
            process.kill()
        process.wait()
        os.close(master)

def terminal_environment(shell, term='xterm-256color', no_color=None):
    env = ShellTest(shell).get_environment()
    env.pop('NO_COLOR', None)
    env.pop('TERM', None)
    if term is not None:
        env['TERM'] = term
    if no_color is not None:
        env['NO_COLOR'] = no_color
    return env

@pytest.mark.skipif(os.name == 'nt', reason="requires a POSIX pseudo-terminal")
@pytest.mark.parametrize("term, no_color, arguments", [
    (None, '1', ['-version']),
    (None, '1', ['-json', '-c', 'SELECT 42']),
    (None, '1', ['-agent', '-c', 'SELECT 42']),
    (None, '1', ['-dark-mode', '-manual', 'list_contains']),
    (None, '1', ['-dark-mode', '-version']),
    (None, None, ['-c', 'SELECT 42']),
    (None, None, ['-help']),
    ('', None, ['-c', 'SELECT 42']),
    ('dumb', None, ['-help']),
    ('dumb', None, ['-c', 'SELECT 42']),
    ('xterm-256color', None, ['-version']),
    ('xterm-256color', None, ['-json', '-version']),
    ('xterm-256color', None, ['-light-mode', '-manual', 'list_contains']),
])
def test_unneeded_color_probe_is_skipped(shell, term, no_color, arguments):
    env = terminal_environment(shell, term, no_color)
    status, output, elapsed = run_terminal(shell, arguments, env)
    assert status == 0, output
    assert b'\x1b]11;?' not in output
    assert b'Timeout trying to read terminal background color' not in output
    assert elapsed < 4

@pytest.mark.skipif(os.name == 'nt', reason="requires a POSIX pseudo-terminal")
def test_command_color_probe_waits_for_terminal(shell):
    env = terminal_environment(shell)
    response = b'\x1b]11;rgb:ffff/ffff/ffff\x07\x1b[?62c'
    status, output, elapsed = run_terminal(shell, ['-c', 'SELECT missing_column'], env, response)
    assert status != 0, output
    assert b'\x1b]11;?' in output
    assert elapsed >= 0.8
    assert b'Timeout trying to read terminal background color' not in output
    assert b'\x1b[38;5;124m' in output  # light-mode error color

@pytest.mark.skipif(os.name == 'nt', reason="requires a POSIX pseudo-terminal")
def test_help_detects_terminal_colors(shell):
    env = terminal_environment(shell)
    response = b'\x1b]11;rgb:ffff/ffff/ffff\x07\x1b[?62c'
    status, output, _ = run_terminal(shell, ['-help'], env, response)
    assert status == 0, output
    assert b'\x1b]11;?' in output
    assert b'\x1b[38;5;27m' in output  # light-mode keyword color

# fmt: on
