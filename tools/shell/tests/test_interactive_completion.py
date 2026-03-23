import os
import pty
import re
import select
import subprocess
import time

import pytest


ANSI_ESCAPE = re.compile(rb"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
CPR_REQUEST = b"\x1b[6n"
CPR_RESPONSE = b"\x1b[1;1R"


def strip_ansi(output: bytes) -> str:
    return ANSI_ESCAPE.sub(b"", output).decode("utf8", errors="ignore")


def read_from_pty(master_fd: int, timeout: float) -> bytes:
    ready, _, _ = select.select([master_fd], [], [], timeout)
    if not ready:
        return b""
    chunk = os.read(master_fd, 4096)
    if CPR_REQUEST in chunk:
        os.write(master_fd, CPR_RESPONSE)
        chunk = chunk.replace(CPR_REQUEST, b"")
    return chunk


def drain_output(master_fd: int, quiet_period: float = 0.2, timeout: float = 5) -> str:
    chunks = bytearray()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        remaining = min(quiet_period, deadline - time.monotonic())
        if remaining <= 0:
            break
        chunk = read_from_pty(master_fd, remaining)
        if not chunk:
            break
        chunks.extend(chunk)
    return strip_ansi(bytes(chunks))


def read_until_contains(master_fd: int, needle: str, timeout: float = 5) -> str:
    chunks = bytearray()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        remaining = min(0.1, deadline - time.monotonic())
        if remaining <= 0:
            break
        chunk = read_from_pty(master_fd, remaining)
        if not chunk:
            continue
        chunks.extend(chunk)
        output = strip_ansi(bytes(chunks))
        if needle in output:
            return output
    return strip_ansi(bytes(chunks))


@pytest.mark.skipif(os.name == "nt", reason="PTY-based interactive shell test")
def test_dot_command_completion_executes_on_enter(shell, tmp_path):
    env = os.environ.copy()
    env["HOME"] = tmp_path.as_posix()
    env["TERM"] = env.get("TERM", "xterm")

    master_fd, slave_fd = pty.openpty()
    proc = subprocess.Popen(
        [shell, "--no-init"],
        stdin=slave_fd,
        stdout=slave_fd,
        stderr=slave_fd,
        env=env,
        close_fds=True,
    )
    os.close(slave_fd)

    attached_db = "enter_once_completion_db"
    try:
        read_until_contains(master_fd, " D ", timeout=5)

        os.write(master_fd, f"ATTACH ':memory:' AS {attached_db};\r".encode("utf8"))
        read_until_contains(master_fd, " D ", timeout=5)

        os.write(master_fd, b".dat")
        drain_output(master_fd)
        os.write(master_fd, b"\t")
        drain_output(master_fd)
        os.write(master_fd, b"\r")
        output = read_until_contains(master_fd, attached_db, timeout=5)

        assert attached_db in output
    finally:
        try:
            os.write(master_fd, b"\r.quit\r")
        except OSError:
            pass
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
        os.close(master_fd)
