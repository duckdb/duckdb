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

@pytest.mark.skipif(os.name == 'nt', reason="requires a POSIX pseudo-terminal")
@pytest.mark.parametrize("arguments", [
    ['-version'],
    ['-help'],
    ['-json', '-c', 'SELECT 42'],
    ['-dark-mode', '-manual', 'list_contains'],
])
def test_unanswered_color_probe_does_not_stall(shell, arguments):
    import pty
    import time

    # nothing answers the background color query on this pseudo-terminal
    master, slave = pty.openpty()
    start = time.monotonic()
    try:
        process = subprocess.Popen(
            [shell, '-no-init', *arguments],
            stdin=slave,
            stdout=slave,
            stderr=slave,
            env=ShellTest(shell).get_environment(),
        )
    finally:
        os.close(slave)
    output = bytearray()
    while True:
        try:
            chunk = os.read(master, 4096)
        except OSError:
            break
        if not chunk:
            break
        output.extend(chunk)
    os.close(master)
    assert process.wait(timeout=30) == 0, output.decode('utf8', 'replace')
    assert b'Timeout trying to read terminal background color' not in output
    assert time.monotonic() - start < 4

@pytest.mark.skipif(os.name == 'nt', reason="requires a POSIX pseudo-terminal")
@pytest.mark.parametrize("events", [
    # the terminal answers the color query after the probe gave up
    [(1.0, b'\x1b]11;rgb:ffff/ffff/ffff\x07\x1b[?62c')],
    # ALT+] starts like an OSC reply, but the keys typed after it must be kept
    [(0.0, b'\x1b]11;rgb:ffff/ffff/ffff\x07\x1b[?62c'), (0.5, b'\x1b]')],
])
def test_terminal_reply_is_not_typed(shell, tmp_path, events):
    import pty
    import re
    import select
    import time

    query = (events[-1][0] + 0.5, b"SELECT 'typed_ok' AS r;\r")
    events = events + [query, (query[0] + 1.5, b'.exit\r')]
    env = ShellTest(shell).get_environment()
    env['DUCKDB_HISTORY'] = str(tmp_path / 'history')
    master, slave = pty.openpty()
    try:
        process = subprocess.Popen([shell, '-no-init'], stdin=slave, stdout=slave, stderr=slave, env=env)
    finally:
        os.close(slave)
    output = bytearray()
    probe_time = None
    deadline = time.monotonic() + 30
    try:
        while process.poll() is None and time.monotonic() < deadline:
            if select.select([master], [], [], 0.05)[0]:
                try:
                    chunk = os.read(master, 65536)
                except OSError:
                    break
                output.extend(chunk)
                # answer cursor position queries like a terminal emulator
                for _ in range(chunk.count(b'\x1b[6n')):
                    os.write(master, b'\x1b[1;1R')
            if probe_time is None and b'\x1b]11;?' in output:
                probe_time = time.monotonic()
            while probe_time is not None and events and time.monotonic() - probe_time >= events[0][0]:
                os.write(master, events.pop(0)[1])
    finally:
        if process.poll() is None:
            process.kill()
        process.wait()
        os.close(master)
    text = re.sub(r'\x1b\[[0-9;?]*[A-Za-z]|\x1b\][^\x07]*\x07', '', output.decode('utf8', 'replace'))
    assert process.returncode == 0, text
    assert '│ typed_ok │' in text, text
    assert 'Error' not in text, text

# fmt: on
