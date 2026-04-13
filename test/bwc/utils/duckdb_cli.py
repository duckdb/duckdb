#!/usr/bin/env python3
"""
DuckDB CLI Interface
"""

import re
from utils.logger import make_logger
from typing import Dict, Any
import os
import subprocess
import threading
import time
import uuid

logger = make_logger(__name__)


class DuckDBCLI:
    """DuckDB CLI interface using CSV output and a unique marker."""

    def __init__(self, duckdb_path: str = None, unsigned: bool = False):
        self.duckdb_path = duckdb_path
        self.unsigned = unsigned
        self.process = None
        self.output_buffer = []
        self.error_buffer = []
        self.output_thread = None
        self.error_thread = None
        self.output_lock = threading.Lock()
        self.error_lock = threading.Lock()
        self.id = os.path.basename(duckdb_path)
        self.version = None
        self.start()

    def start(self):
        try:
            args = [self.duckdb_path, "-batch", "-csv"]
            if self.unsigned:
                args.append("-unsigned")

            self.process = subprocess.Popen(
                args,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                universal_newlines=True,
            )
            self.output_buffer = []
            self.error_buffer = []
            self.output_thread = threading.Thread(target=self._monitor_stdout, daemon=True)
            self.error_thread = threading.Thread(target=self._monitor_stderr, daemon=True)
            self.output_thread.start()
            self.error_thread.start()
            time.sleep(0.05)
            if self.process.poll() is not None:
                raise RuntimeError(f"DuckDB CLI '{self.id}' process failed to start")

            version_result = self.execute_command("select library_version from pragma_version();")
            self.version = version_result['output'][1]
        except Exception as e:
            raise RuntimeError(f"Failed to start DuckDB CLI '{self.id}': {e}")

    def _monitor_stdout(self):
        while self.process and self.process.poll() is None:
            line = self.process.stdout.readline()
            if not line:
                break
            with self.output_lock:
                self.output_buffer.append(line.rstrip('\n'))

    def _monitor_stderr(self):
        while self.process and self.process.poll() is None:
            line = self.process.stderr.readline()
            if not line:
                break
            with self.error_lock:
                self.error_buffer.append(line.rstrip('\n'))

    def _collect_crash_output(self):
        """Collect any remaining output from a crashed process."""
        if not self.process:
            return [], [], None

        crash_stderr = []
        crash_stdout = []

        # Give monitor threads a moment to flush remaining output
        time.sleep(0.1)
        with self.output_lock:
            crash_stdout = self.output_buffer.copy()
        with self.error_lock:
            crash_stderr = self.error_buffer.copy()
        # Also try to read any remaining data directly
        try:
            remaining_stderr = self.process.stderr.read()
            if remaining_stderr:
                crash_stderr.extend(remaining_stderr.strip().splitlines())
        except Exception:
            pass
        try:
            remaining_stdout = self.process.stdout.read()
            if remaining_stdout:
                crash_stdout.extend(remaining_stdout.strip().splitlines())
        except Exception:
            pass

        return crash_stdout, crash_stderr, self.process.returncode

    def _restart(self):
        """Restart the CLI process after a crash."""
        logger.info(f"[{self.version}] Restarting CLI process after crash")
        try:
            if self.process:
                self.process.kill()
                self.process.wait()
        except Exception:
            pass
        self.process = None
        self.start()

    def _make_crash_result(self, command, crash_stdout, crash_stderr, return_code):
        """Build a failure result from crash output."""
        crash_details = []
        if return_code is not None:
            crash_details.append(f"Process exited with code {return_code}")
        if crash_stderr:
            crash_details.append("stderr:\n" + "\n".join(crash_stderr))
        if crash_stdout:
            crash_details.append("stdout:\n" + "\n".join(crash_stdout))
        crash_msg = "\n".join(crash_details) if crash_details else "CLI process crashed (no output captured)"
        logger.warning(f"[{self.version}] CLI process crashed during command execution:\n{crash_msg}")
        return {
            "query": command,
            "success": False,
            "output": crash_stdout,
            "error": crash_msg,
            "exception_message": f"CLI process crashed: {crash_msg}",
            "error_log": f"[{self.version}] ❌ Command: \n-------\n{command}\n-------\nfailed with:\n{crash_msg}",
        }

    def execute_command(self, command: str) -> Dict[str, Any]:
        if not self.process or self.process.poll() is not None:
            raise RuntimeError(f"DuckDB CLI '{self.id}' process is not running")

        # Unique marker for this command
        marker = f"__END__{uuid.uuid4().hex}__"
        marker_sql = f"SELECT '{marker}';"
        # Clear previous output
        with self.output_lock:
            self.output_buffer.clear()
        with self.error_lock:
            self.error_buffer.clear()
        # Send command and marker
        if not command.strip().endswith(';'):
            command = command.strip() + ';'
        try:
            self.process.stdin.write(command + '\n')
            self.process.stdin.write(marker_sql + '\n')
            self.process.stdin.flush()
        except (BrokenPipeError, OSError):
            crash_stdout, crash_stderr, return_code = self._collect_crash_output()
            self._restart()
            return self._make_crash_result(command, crash_stdout, crash_stderr, return_code)
        # Read output until marker is seen
        output_lines = []
        found_marker = False
        start_time = time.time()
        timeout = 10
        while time.time() - start_time < timeout:
            with self.output_lock:
                while self.output_buffer:
                    line = self.output_buffer.pop(0)
                    if marker in line:
                        found_marker = True
                        break
                    output_lines.append(line)
            with self.error_lock:
                if len(self.error_buffer) > 0:
                    found_marker = True

            if found_marker:
                break
            time.sleep(0.01)

        # Wait for the whole output to be processed
        last_output_time = time.time()
        last_stdout_count = len(self.output_buffer)
        last_stderr_count = len(self.error_buffer)
        while True:
            with self.output_lock:
                if last_stdout_count < len(self.output_buffer):
                    last_stdout_count = len(self.output_buffer)
                    last_output_time = time.time()
            with self.error_lock:
                if last_stderr_count < len(self.error_buffer):
                    last_stderr_count = len(self.error_buffer)
                    last_output_time = time.time()
            if time.time() - last_output_time > 0.05:  # TODO review that
                break

        # Collect any errors
        with self.error_lock:
            error_lines = self.error_buffer.copy()
        # Remove the marker header if present (CSV header for marker)
        if output_lines and output_lines[-1] == marker:
            output_lines = output_lines[:-1]
        # Remove empty lines at the end
        while output_lines and output_lines[-1] == '':
            output_lines.pop()

        success = found_marker and not DuckDBCLI.has_fatal_error(error_lines)
        result = {
            "query": command,
            "success": success,
            "output": output_lines,
            "error": "\n".join(error_lines) if error_lines else None,
        }

        if result["success"]:
            if len(result["output"]) == 0 or (
                len(result["output"]) == 2 and result["output"][0] == "result" and result["output"][1] == "true"
            ):
                logger.debug(f"[{self.version}] ✅ '{command}'")
            else:
                logger.debug(f"[{self.version}] ✅ '{command}' - result:")
            for line in result["output"]:
                logger.debug(f"[{self.version}]  {line}")
        else:
            output_lines = [x for x in result["output"] if x is not None]
            if result['error'] is not None:
                output_lines.append(result['error'])
            output_lines = '\n'.join(output_lines)

            if '"exception_message":' in output_lines:
                # Extract the exception message from the JSON output
                match = re.search(r'"exception_message":"((?:[^"\\]|\\.)*)"', output_lines)
                if match:
                    result['exception_message'] = match.group(1).split('\\n')[
                        0
                    ]  # Get the first line of the exception message

            if 'exception_message' not in result:
                if result['error'] is not None:
                    error_lines = result['exception_message'] = result['error'].splitlines()
                    if 'failed with message' in error_lines[0]:
                        result['exception_message'] = error_lines[1]
                    else:
                        result['exception_message'] = error_lines[0]
                else:
                    result['exception_message'] = output_lines
            result['error_log'] = (
                f"[{self.version}] ❌ Command: \n-------\n{command}\n-------\nfailed with:\n{output_lines}"
            )
        return result

    @staticmethod
    def has_fatal_error(error_lines) -> bool:
        if not error_lines:
            return False
        for line in error_lines:
            if "terminating due to" in line or "Error" in line:
                return True
        return False

    def close(self):
        if not self.process or self.process.poll() is not None:
            return

        self.process.stdin.write(".quit\n")
        self.process.stdin.flush()
        time.sleep(0.05)
        if self.process.poll() is not None:
            return

        self.process.terminate()
        time.sleep(0.2)

        if self.process.poll() is None:
            self.process.kill()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
