#!/usr/bin/env python3
import argparse
import concurrent.futures
import contextlib
import os
import re
import shlex
import signal
import subprocess
import sys
import tempfile
import threading
import time
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import asdict, dataclass
from io import StringIO
from pathlib import Path

DEFAULT_BATCH_SIZE = 10
DEFAULT_BATCH_TIMEOUT_SECONDS = 600
HIGH_WORKER_BATCH_TIMEOUT_SECONDS = 300
HIGH_WORKER_BATCH_TIMEOUT_THRESHOLD = 10
DEFAULT_RSS_MEMORY_THRESHOLD_MIB = 1024
DEFAULT_RUNTIME_THRESHOLD_SECONDS = 10
DEFAULT_RSS_POLL_INTERVAL_SECONDS = 0.05
# Leave some CPU headroom so parallel test execution does not fully saturate CI runners.
DEFAULT_WORKERS = "75%"
DEFAULT_MAX_RETRIES = 4
STABILIZE_SLOW_TOTAL_RUNS = 3
STABILIZE_FAST_TOTAL_RUNS = 10
STABILIZE_FAST_TOTAL_RUNS_LARGE = 3
STABILIZE_CHANGED_TEST_THRESHOLD = 500
STOP_REQUESTED = threading.Event()


def enable_line_buffering():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True, write_through=True)
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(line_buffering=True, write_through=True)


def signal_stop_requested(signum, frame):
    STOP_REQUESTED.set()


def stop_requested():
    return STOP_REQUESTED.is_set()


@dataclass(frozen=True)
class TestRunnerConfig:
    test_list: Path
    unittest_bin: str
    test_flags: str
    patterns: list[str]
    test_command: str
    workers: int
    retry: int
    max_retries: int
    batch_size: int
    batch_timeout_seconds: float
    rss_memory_threshold_mib: int | None
    runtime_threshold_seconds: int | None
    max_failures: int | None
    fail_require_skip: bool


@dataclass(frozen=True)
class TestCase:
    name: str
    is_slow: bool


@dataclass(frozen=True)
class FailedAttempt:
    lines: list[str]
    reproduce_batch: list[str]


@dataclass(frozen=True)
class FailureInfo:
    kind: str
    test_name: str | None
    line_number: int | None
    mismatch_context: str | None
    expected: str | None
    actual: str | None
    snippet_lines: list[str]
    detail_lines: list[str]
    timeout_seconds: float | None
    reproduce_batch: list[str]


class BatchRunState:
    def __init__(self):
        self.failed_count = 0
        self.retry_count = 0
        self.stop_launching = False
        self.failed_attempts = {}

    def record_retry(self):
        self.retry_count += 1

    def record_failure(self):
        self.failed_count += 1

    def add_failed_attempt(self, batch_idx: int, lines: list[str], reproduce_batch: list[str]):
        self.failed_attempts.setdefault(batch_idx, []).append(
            FailedAttempt(lines=lines, reproduce_batch=reproduce_batch)
        )

    def pop_failed_attempts(self, batch_idx: int):
        return self.failed_attempts.pop(batch_idx, [])

    def can_retry(self, batch_info, config: TestRunnerConfig):
        return batch_info["attempt"] < config.retry and self.retry_count < config.max_retries

    def should_stop(self, config: TestRunnerConfig):
        return self.stop_launching or config.max_failures is not None and self.failed_count >= config.max_failures


class DotProgressBar:
    def __init__(self, total_batches: int):
        self.total_batches = total_batches
        self.printed_dots = 0
        self.row_width = 50
        self._line_open = False

    def _write(self, text: str):
        sys.stdout.write(text)
        sys.stdout.flush()

    def flush_line(self):
        if self._line_open:
            self._write("\n")
            self._line_open = False

    def print_message(self, text: str):
        self.flush_line()
        print(text)

    def advance(self, completed_batches: int):
        if self.total_batches <= 0:
            return
        target_dots = min(100, int((completed_batches * 100) / self.total_batches))
        while self.printed_dots < target_dots:
            self._write(".")
            self.printed_dots += 1
            self._line_open = True
            if self.printed_dots % self.row_width == 0:
                self._write(" [{:3}%]\n".format(self.printed_dots))
                self._line_open = False


@dataclass
class RunContext:
    executor: concurrent.futures.ThreadPoolExecutor
    config: TestRunnerConfig
    state: BatchRunState
    future_to_batch: dict
    progress: DotProgressBar


def chunked(items, n):
    # Keep input order, cap batches at n entries, and isolate slow tests so
    # each batch contains at most one slow test.
    batch = []
    slow_count = 0
    for item in items:
        if batch and (len(batch) >= n or (item.is_slow and slow_count >= 1)):
            yield batch
            batch = []
            slow_count = 0
        batch.append(item.name)
        if item.is_slow:
            slow_count += 1
    if batch:
        yield batch


def compute_batch_size(test_count: int, config: TestRunnerConfig):
    if test_count == 0:
        return 1
    return max(1, min(config.batch_size, (test_count + config.workers - 1) // config.workers))


def split_fast_slow_tests(tests: list[TestCase]):
    fast_tests = [test for test in tests if not test.is_slow]
    slow_tests = [test for test in tests if test.is_slow]
    return fast_tests, slow_tests


def stabilization_extra_runs(candidate_count: int):
    fast_total_runs = STABILIZE_FAST_TOTAL_RUNS
    if candidate_count > STABILIZE_CHANGED_TEST_THRESHOLD:
        fast_total_runs = STABILIZE_FAST_TOTAL_RUNS_LARGE
    slow_total_runs = STABILIZE_SLOW_TOTAL_RUNS
    return max(0, fast_total_runs - 1), max(0, slow_total_runs - 1)


def load_tests(path: Path):
    tests = []
    with path.open("r", encoding="utf8") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue

            # Skip header row from `--list-tests`.
            if line == "name\tgroup":
                continue

            if "\t" not in line:
                name = line
                is_slow = line.endswith(".test_slow")
            else:
                columns = line.split("\t")
                assert len(columns) == 2, repr(columns)

                name, group = columns
                is_slow = "[.]" in group
                assert not name.endswith(".test_slow") or is_slow, name

            tests.append(TestCase(name=name, is_slow=is_slow))

    return tests


def build_test_command(config: TestRunnerConfig, test_list: str):
    flags = shlex.join(shlex.split(config.test_flags))
    return config.test_command.format(
        binary=shlex.quote(config.unittest_bin),
        flags=flags,
        test_list=test_list,
    )


def get_process_rss_bytes(pid: int):
    if sys.platform.startswith("linux"):
        try:
            with open(f"/proc/{pid}/status", "r", encoding="utf8") as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        fields = line.split()
                        return int(fields[1]) * 1024
        except (FileNotFoundError, ProcessLookupError, ValueError):
            return None
        return None

    if sys.platform == "darwin":
        try:
            proc = subprocess.run(
                ["ps", "-o", "rss=", "-p", str(pid)],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
                encoding="utf8",
                errors="backslashreplace",
            )
        except OSError:
            return None

        if proc.returncode != 0:
            return None

        output = proc.stdout.strip()
        if not output:
            return None

        try:
            return int(output) * 1024
        except ValueError:
            return None

    return None


def format_mib(value_bytes: int):
    return value_bytes / (1024 * 1024)


def resolve_workers(workers: str):
    cpu_count = os.cpu_count() or 1
    workers = workers.strip()
    if workers.endswith("%"):
        percentage = int(workers[:-1])
        return max(1, int(cpu_count * (percentage / 100.0)))
    return max(1, int(workers))


def resolve_batch_timeout(batch_timeout: float | None, workers: int):
    if batch_timeout is not None:
        return batch_timeout
    if workers >= HIGH_WORKER_BATCH_TIMEOUT_THRESHOLD:
        return HIGH_WORKER_BATCH_TIMEOUT_SECONDS
    return DEFAULT_BATCH_TIMEOUT_SECONDS


def generate_test_list(
    test_file,
    unittest_bin: str,
    test_flags: str,
    patterns: list[str],
    test_list_files: list[Path] | None = None,
):
    # Catch can return a non-zero status code for list commands when tests
    # are found, so we accept non-zero if stdout still contains test output.
    list_file_args = []
    if test_list_files:
        list_file_args = [arg for test_list_file in test_list_files for arg in ("-f", str(test_list_file))]
    command = [unittest_bin, *shlex.split(test_flags), "--list-tests", *list_file_args, *patterns]
    proc = subprocess.run(
        command,
        text=True,
        encoding="utf8",
        errors="backslashreplace",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if proc.returncode != 0 and not proc.stdout:
        print("Stderr:", proc.stderr, end="", file=sys.stderr, flush=True)
        raise RuntimeError(f"failed to generate test list from {unittest_bin} (exit: {proc.returncode})")
    test_file.write(proc.stdout)
    test_file.flush()


@contextlib.contextmanager
def open_test_list(
    test_list: Path | None,
    unittest_bin: str,
    test_flags: str,
    patterns: list[str],
    test_list_files: list[Path] | None = None,
):
    if test_list is not None and (test_list_files is None or len(test_list_files) == 1):
        yield test_list
        return

    with tempfile.NamedTemporaryFile(mode="w", encoding="utf8", delete=False) as test_file:
        generate_test_list(test_file, unittest_bin, test_flags, patterns, test_list_files)
    result = Path(test_file.name)
    yield result
    result.unlink()


def format_duration_seconds(value: float):
    if value.is_integer():
        return str(int(value))
    return f"{value:g}"


def render_test_snippet(test_name: str | None, line_number: int | None):
    if not test_name or line_number is None or line_number <= 0:
        return []
    test_path = Path(test_name)
    if not test_path.exists():
        return []
    try:
        file_lines = test_path.read_text(encoding="utf8").splitlines()
    except OSError:
        return []

    start_idx = line_number - 1
    while start_idx > 0 and file_lines[start_idx - 1].strip():
        start_idx -= 1
    end_idx = line_number
    while end_idx < len(file_lines) and file_lines[end_idx].strip():
        end_idx += 1
    window = [(idx + 1, file_lines[idx]) for idx in range(start_idx, end_idx)]
    while window and not window[0][1].strip():
        window.pop(0)
    while window and not window[-1][1].strip():
        window.pop()
    if not window:
        return []

    width = len(str(window[-1][0]))
    rendered = []
    for lineno, text in window:
        marker = ">" if lineno == line_number else " "
        rendered.append(f"  {marker} {lineno:>{width}}  {text}")
    return rendered


FAILING_TEST_PATTERN = re.compile(r"^\d+\.\s+(.+?):(\d+)$")
ERROR_LINE_LOCATION_PATTERN = re.compile(r"\((.+?):(\d+)\)!?$")
WRONG_RESULT_PATTERN = re.compile(r"^(?:Error:\s+)?Wrong result in query!\s*(?:\((.+?):(\d+)\)!)?$")
PROGRESS_TEST_START_PATTERN = re.compile(r"^\[\d+/\d+\] \(\d+%\): (.+)$")
FATAL_ERROR_PATTERN = re.compile(r"^\s*due to a fatal error condition:\s*$")
FAILED_HEADER_PATTERN = re.compile(r"^\s*.+:\s+FAILED:\s*$")
EXPLICIT_MESSAGE_PATTERN = re.compile(r"^\s*explicitly with message:\s*$")
SANITIZER_OR_ASSERT_PATTERN = re.compile(
    r"(AddressSanitizer|LeakSanitizer|ThreadSanitizer|UndefinedBehaviorSanitizer|runtime error:|assert)",
    flags=re.IGNORECASE,
)


def find_failing_test(stderr_lines: list[str], batch):
    batch_test_name = batch[0] if len(batch) == 1 else None
    test_name = batch_test_name
    line_number = None
    for line in stderr_lines:
        match = FAILING_TEST_PATTERN.match(line.strip())
        if match:
            test_name = match.group(1)
            line_number = int(match.group(2))
            break
    return test_name, line_number


def find_failing_test_from_stdout(stdout_lines: list[str], batch):
    test_name = batch[0] if len(batch) == 1 else None
    for line in stdout_lines:
        stripped = line.strip()
        match = FAILING_TEST_PATTERN.match(stripped)
        if match:
            test_name = match.group(1)
            break
        progress_match = PROGRESS_TEST_START_PATTERN.match(stripped)
        if progress_match and " took " not in progress_match.group(1):
            test_name = progress_match.group(1)
    return test_name


def infer_timed_out_test_from_stdout(stdout_lines: list[str], batch):
    started_tests = []
    completed_tests = set()

    for line in stdout_lines:
        stripped = line.strip()
        progress_match = PROGRESS_TEST_START_PATTERN.match(stripped)
        if not progress_match:
            continue
        progress_text = progress_match.group(1)
        if " took " in progress_text:
            completed_tests.add(progress_text.split(" took ", 1)[0])
        else:
            started_tests.append(progress_text)

    for test_name in started_tests:
        if test_name not in completed_tests:
            return test_name
    if completed_tests and batch:
        for test_name in batch:
            if test_name not in completed_tests:
                return test_name
    if started_tests:
        return started_tests[-1]
    if batch:
        return batch[-1]
    return None


def extract_failed_reason_line(stdout_lines: list[str]):
    for idx, line in enumerate(stdout_lines):
        if not FAILED_HEADER_PATTERN.match(line):
            continue

        for lookahead_idx in range(idx + 1, len(stdout_lines)):
            stripped = stdout_lines[lookahead_idx].strip()
            if not stripped:
                continue
            if FATAL_ERROR_PATTERN.match(stdout_lines[lookahead_idx]):
                for next_line in stdout_lines[lookahead_idx + 1 :]:
                    next_stripped = next_line.strip()
                    if next_stripped:
                        return next_stripped
                return None
            if EXPLICIT_MESSAGE_PATTERN.match(stdout_lines[lookahead_idx]):
                for next_line in stdout_lines[lookahead_idx + 1 :]:
                    next_stripped = next_line.strip()
                    if next_stripped:
                        return next_stripped
                return None
            if stripped.startswith("{") and stripped.endswith("}"):
                continue
            return stripped
    return None


def extract_failing_stderr_block(stderr_lines: list[str]):
    start_idx = None
    for idx, line in enumerate(stderr_lines):
        stripped = line.strip()
        if FAILING_TEST_PATTERN.match(stripped) or WRONG_RESULT_PATTERN.match(stripped):
            start_idx = idx
            break
    if start_idx is None:
        return []

    end_idx = len(stderr_lines)
    for idx in range(start_idx + 1, len(stderr_lines)):
        if stderr_lines[idx].startswith("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"):
            end_idx = idx
            break

    block = [line.rstrip() for line in stderr_lines[start_idx:end_idx]]
    while block and not block[0].strip():
        block.pop(0)
    while block and not block[-1].strip():
        block.pop()
    return block


def extract_interesting_failure_block(lines: list[str]):
    for idx, line in enumerate(lines):
        if not SANITIZER_OR_ASSERT_PATTERN.search(line):
            continue
        block = []
        for next_line in lines[idx:]:
            stripped = next_line.strip()
            if not stripped:
                if block:
                    break
                continue
            block.append(stripped)
            if len(block) >= 3:
                break
        if block:
            return block
    return []


def format_signal_summary(returncode: int | None):
    if returncode is None or returncode >= 0:
        return None
    signal_number = -returncode
    try:
        signal_name = signal.Signals(signal_number).name
    except ValueError:
        signal_name = f"SIG{signal_number}"
    description = signal.strsignal(signal_number) or "terminated by signal"
    return f"{signal_name} - {description}"


def parse_failure_info(message: str | None, stdout: str, stderr: str, batch, returncode: int | None = None):
    stderr_lines = strip_ansi(stderr).splitlines()
    stdout_lines = strip_ansi(stdout).splitlines()
    stderr_non_empty_lines = [line.strip() for line in stderr_lines if line.strip()]
    stdout_non_empty_lines = [line.strip() for line in stdout_lines if line.strip()]
    batch_test_name = batch[0] if len(batch) == 1 else None

    if message is not None and message.startswith("batch timed out after "):
        timeout_test_name = batch_test_name or infer_timed_out_test_from_stdout(stdout_lines, batch)
        reproduce_batch = [timeout_test_name] if timeout_test_name else list(batch)
        return FailureInfo(
            kind="timeout",
            test_name=timeout_test_name,
            line_number=None,
            mismatch_context=None,
            expected=None,
            actual=None,
            snippet_lines=[],
            detail_lines=[],
            timeout_seconds=float(re.search(r"after ([0-9]+(?:\.[0-9]+)?) seconds", message).group(1)),
            reproduce_batch=reproduce_batch,
        )

    test_name, line_number = find_failing_test(stderr_lines, batch)
    if test_name is None and returncode is not None and returncode < 0:
        test_name = infer_timed_out_test_from_stdout(stdout_lines, batch)
    reproduce_batch = [test_name] if test_name else list(batch)

    error_line = next((line for line in stderr_non_empty_lines if WRONG_RESULT_PATTERN.match(line)), None)
    if error_line is not None:
        if line_number is None:
            location_match = WRONG_RESULT_PATTERN.match(error_line)
            if location_match:
                if location_match.group(1):
                    test_name = test_name or location_match.group(1)
                if location_match.group(2):
                    line_number = int(location_match.group(2))
        mismatch_context = next((line for line in stderr_non_empty_lines if line.startswith("Mismatch on row ")), None)
        mismatch_line = next((line for line in stderr_non_empty_lines if "<>" in line), None)
        actual = None
        expected = None
        if mismatch_line is not None:
            actual, expected = [part.strip() for part in mismatch_line.split("<>", 1)]
        return FailureInfo(
            kind="wrong_result",
            test_name=test_name,
            line_number=line_number,
            mismatch_context=mismatch_context,
            expected=expected,
            actual=actual,
            snippet_lines=render_test_snippet(test_name, line_number),
            detail_lines=[],
            timeout_seconds=None,
            reproduce_batch=reproduce_batch,
        )

    detail_lines = []
    failing_stderr_block = extract_failing_stderr_block(stderr_lines)
    if failing_stderr_block:
        detail_lines.extend(failing_stderr_block)
    if not detail_lines:
        detail_lines.extend(extract_interesting_failure_block(stderr_lines))
    if not detail_lines:
        detail_lines.extend(extract_interesting_failure_block(stdout_lines))
    if message is not None:
        if not detail_lines:
            detail_lines.append(message)
    if not detail_lines:
        for line in stderr_non_empty_lines:
            if line.startswith("Error: "):
                detail_lines.append(line.removeprefix("Error: ").strip())
                break
    if not detail_lines:
        failed_reason_line = extract_failed_reason_line(stdout_lines)
        if failed_reason_line is not None:
            test_name = find_failing_test_from_stdout(stdout_lines, batch) or test_name
            reproduce_batch = [test_name] if test_name else list(batch)
            detail_lines.append(failed_reason_line)
    if not detail_lines:
        signal_summary = format_signal_summary(returncode)
        if signal_summary is not None:
            detail_lines.append(signal_summary)
    if not detail_lines:
        first_line = next((line for line in stderr_non_empty_lines if line), None)
        if first_line is not None:
            detail_lines.append(first_line)
    if not detail_lines:
        first_line = next((line for line in stdout_non_empty_lines if line), None)
        if first_line is not None:
            detail_lines.append(first_line)
    return FailureInfo(
        kind="generic",
        test_name=test_name,
        line_number=line_number,
        mismatch_context=None,
        expected=None,
        actual=None,
        snippet_lines=[],
        detail_lines=detail_lines,
        timeout_seconds=None,
        reproduce_batch=reproduce_batch,
    )


def render_failure_lines(failure: FailureInfo):
    if failure.kind == "timeout":
        test_name = failure.test_name or "test batch"
        return [f"error: timeout ({format_duration_seconds(failure.timeout_seconds)}s) for {test_name}."]

    if failure.kind == "wrong_result":
        test_name = failure.test_name or "test batch"
        lines = [f"error: FAIL {test_name}", ""]
        if failure.expected is not None and failure.actual is not None and failure.mismatch_context is not None:
            lines.append(f"expected: {failure.expected}, got {failure.actual}; {failure.mismatch_context}")
        elif failure.expected is not None and failure.actual is not None:
            lines.append(f"expected: {failure.expected}, got {failure.actual}")
        elif failure.mismatch_context is not None:
            lines.append(failure.mismatch_context)
        if failure.snippet_lines:
            lines.extend(["", *failure.snippet_lines])
        return lines

    if failure.test_name:
        lines = [f"error: FAIL {failure.test_name}"]
    else:
        lines = ["error: test batch failed"]
    if failure.detail_lines:
        lines.extend(["", *failure.detail_lines])
    return lines


def format_batch_failure(batch, config: TestRunnerConfig, attempt_summaries, recovered: bool, retry_count: int):
    reproduce_batch = batch
    rerun_parts = [shlex.quote(format_unittest_bin_for_display(config.unittest_bin))]
    rerun_parts.extend(shlex.split(config.test_flags))
    parts = []
    if recovered:
        first_attempt = attempt_summaries[0]
        parts.extend(first_attempt.lines)
        parts.extend(["", f"recovered: passed on retry {retry_count}/{config.retry}"])
        reproduce_batch = first_attempt.reproduce_batch
    else:
        last_attempt = attempt_summaries[-1]
        parts.extend(last_attempt.lines)
        reproduce_batch = last_attempt.reproduce_batch
    rerun_parts.append(",".join(reproduce_batch))
    rerun_cmd = shlex.join(rerun_parts)
    parts.extend(["", "reproduce:", rerun_cmd, ""])
    return "\n".join(parts)


def format_unittest_bin_for_display(unittest_bin: str):
    try:
        if os.path.isabs(unittest_bin):
            return os.path.relpath(unittest_bin, os.getcwd())
    except ValueError:
        # On Windows, relpath can fail across drives. Fall back to the original path.
        return unittest_bin
    return unittest_bin


def normalize_output(output):
    if isinstance(output, bytes):
        return output.decode("utf8", errors="backslashreplace")
    return output or ""


SKIPPED_TESTS_PATTERN = re.compile(
    r"(?:All tests passed \(|All tests were skipped \(total skipped )(\d+)(?: skipped tests,|\))"
)
SKIP_REASON_PATTERN = re.compile(r"(.+):\s+(\d+)$")
MODE_SKIP_REASON_PATTERN = re.compile(r"^mode skip(?:\s+(.*\S))?\s*$")
ANSI_ESCAPE_PATTERN = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")
TEST_RUNTIME_PATTERN = re.compile(r"^\[\d+/\d+\] \(\d+%\): (.+) took ([0-9]+(?:\.[0-9]+)?)s\s*$")


def strip_ansi(text: str):
    return ANSI_ESCAPE_PATTERN.sub("", text)


def parse_test_runtimes(output: str):
    runtimes = []
    for line in strip_ansi(output).splitlines():
        match = TEST_RUNTIME_PATTERN.match(line.strip())
        if match:
            runtimes.append((match.group(1), float(match.group(2))))
    return runtimes


def extract_test_runtimes(stdout: str, stderr: str):
    return parse_test_runtimes(stdout) + parse_test_runtimes(stderr)


def summarize_failure_output(message: str | None, stdout: str, stderr: str, batch, returncode: int | None = None):
    failure = parse_failure_info(message, stdout, stderr, batch, returncode)
    return render_failure_lines(failure), failure.reproduce_batch


def format_failed_test_retry_target(test_name: str | None, batch_info):
    if test_name:
        return test_name
    if batch_info["batch"]:
        return batch_info["batch"][-1]
    return f"batch {batch_info['batch_idx']}"


def parse_skipped_tests_count(output: str):
    match = SKIPPED_TESTS_PATTERN.search(strip_ansi(output))
    if not match:
        return 0
    return int(match.group(1))


def parse_skipped_test_summary(output: str):
    skipped_count = 0
    reasons = {}
    in_skip_summary = False
    for line in strip_ansi(output).splitlines():
        stripped = line.strip()
        if skipped_count == 0:
            count_match = SKIPPED_TESTS_PATTERN.search(stripped)
            if count_match:
                skipped_count = int(count_match.group(1))
        if stripped == "Skipped tests for the following reasons:":
            in_skip_summary = True
            continue
        if in_skip_summary:
            if not stripped:
                in_skip_summary = False
                continue
            reason_match = SKIP_REASON_PATTERN.match(stripped)
            if not reason_match:
                in_skip_summary = False
                continue
            reasons[reason_match.group(1)] = reasons.get(reason_match.group(1), 0) + int(reason_match.group(2))
    return skipped_count, reasons


def extract_skipped_test_output(stdout: str, stderr: str):
    stdout_summary = parse_skipped_test_summary(stdout)
    if stdout_summary[0] > 0 or stdout_summary[1]:
        return stdout_summary

    stderr_summary = parse_skipped_test_summary(stderr)
    if stderr_summary[0] > 0 or stderr_summary[1]:
        return stderr_summary

    return 0, {}


def run_batch(config: TestRunnerConfig, batch):
    failed = False
    stdout = ""
    stderr = ""
    message = None
    allow_retry = True
    peak_rss_bytes = 0

    # On Windows the child process cannot reopen a NamedTemporaryFile while it
    # is still open here, so keep it after close and unlink it ourselves.
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf8", delete=False) as batch_file:
        batch_file.write("\n".join(batch))
        batch_file.write("\n")
        batch_file.flush()
        batch_file_path = Path(batch_file.name)

    command = build_test_command(config, shlex.quote(str(batch_file_path)))
    try:
        proc = subprocess.Popen(
            shlex.split(command),
            text=True,
            encoding="utf8",
            errors="backslashreplace",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        deadline = time.monotonic() + config.batch_timeout_seconds

        while proc.poll() is None:
            rss_bytes = get_process_rss_bytes(proc.pid)
            if rss_bytes is not None:
                peak_rss_bytes = max(peak_rss_bytes, rss_bytes)
            if time.monotonic() >= deadline:
                proc.kill()
                stdout, stderr = proc.communicate()
                stdout = normalize_output(stdout)
                stderr = normalize_output(stderr)
                failed = True
                message = f"batch timed out after {config.batch_timeout_seconds} seconds"
                break
            if proc.poll() is None:
                time.sleep(DEFAULT_RSS_POLL_INTERVAL_SECONDS)

        if message is None:
            stdout, stderr = proc.communicate()
            stdout = normalize_output(stdout)
            stderr = normalize_output(stderr)
            rss_bytes = get_process_rss_bytes(proc.pid)
            if rss_bytes is not None:
                peak_rss_bytes = max(peak_rss_bytes, rss_bytes)
            failed = proc.returncode != 0
            if not failed and config.fail_require_skip:
                require_skip_lines = find_require_skip_lines(stdout)
                if require_skip_lines:
                    failed = True
                    message = "error: detected require-based skipped tests: " + require_skip_lines[0]
                    allow_retry = False
    except OSError as exc:
        failed = True
        stderr = str(exc)
        message = "failed to launch batch command"
    finally:
        batch_file_path.unlink(missing_ok=True)

    return {
        "failed": failed,
        "stdout": stdout,
        "stderr": stderr,
        "message": message,
        "returncode": proc.returncode if "proc" in locals() else None,
        "peak_rss_bytes": peak_rss_bytes,
        "allow_retry": allow_retry,
    }


def find_require_skip_lines(output: str):
    ansi = r"(?:\x1b\[[0-9;]*m)*"
    pattern = rf"^{ansi}(require\s+\S+:\s+\d+){ansi}\s*$"
    return re.findall(pattern, output, flags=re.MULTILINE)


def submit_batch(executor, config: TestRunnerConfig, batch, future_to_batch, batch_idx: int, attempt: int):
    future = executor.submit(run_batch, config, batch)
    future_to_batch[future] = {
        "batch_idx": batch_idx,
        "batch": batch,
        "start": time.monotonic(),
        "attempt": attempt,
    }


def submit_batches(executor, config: TestRunnerConfig, batches, future_to_batch, next_batch_idx: int):
    while next_batch_idx < len(batches) and len(future_to_batch) < config.workers:
        batch = batches[next_batch_idx]
        submit_batch(executor, config, batch, future_to_batch, next_batch_idx, 0)
        next_batch_idx += 1
    return next_batch_idx


def handle_failed_batch(ctx: RunContext, batch_info, result):
    failure = parse_failure_info(
        result["message"],
        result["stdout"],
        result["stderr"],
        batch_info["batch"],
        result.get("returncode"),
    )
    lines = render_failure_lines(failure)
    reproduce_batch = failure.reproduce_batch
    ctx.state.add_failed_attempt(batch_info["batch_idx"], lines, reproduce_batch)
    retry_target = format_failed_test_retry_target(failure.test_name, batch_info)
    if result.get("allow_retry", True) and ctx.state.can_retry(batch_info, ctx.config):
        ctx.state.record_retry()
        next_attempt = batch_info["attempt"] + 1
        ctx.progress.print_message(
            f"retrying failed test {retry_target} "
            f"(attempt {next_attempt}/{ctx.config.retry}, retry {ctx.state.retry_count}/{ctx.config.max_retries})"
        )
        submit_batch(
            ctx.executor,
            ctx.config,
            batch_info["batch"],
            ctx.future_to_batch,
            batch_info["batch_idx"],
            next_attempt,
        )
        return True

    if result.get("allow_retry", True) and batch_info["attempt"] < ctx.config.retry:
        ctx.progress.print_message(
            f"not retrying failed test {retry_target} after reaching {ctx.config.max_retries} retries"
        )

    ctx.progress.print_message(
        format_batch_failure(
            batch_info["batch"],
            ctx.config,
            ctx.state.pop_failed_attempts(batch_info["batch_idx"]),
            recovered=False,
            retry_count=batch_info["attempt"],
        )
    )
    ctx.state.record_failure()
    if ctx.state.should_stop(ctx.config):
        ctx.state.stop_launching = True
    return False


def report_batch_metrics(ctx: RunContext, batch_info, result, elapsed: float):
    if ctx.config.runtime_threshold_seconds is not None:
        test_runtimes = extract_test_runtimes(result["stdout"], result["stderr"])
        if test_runtimes:
            for test_name, test_elapsed in test_runtimes:
                if test_elapsed >= ctx.config.runtime_threshold_seconds:
                    ctx.progress.print_message(f"warn: {test_name} took {test_elapsed:.2f}s")
        elif elapsed >= ctx.config.runtime_threshold_seconds:
            ctx.progress.print_message(f"warn: {batch_info['batch'][0]} took {elapsed:.2f}s")
    if (
        ctx.config.rss_memory_threshold_mib is not None
        and format_mib(result["peak_rss_bytes"]) >= ctx.config.rss_memory_threshold_mib
    ):
        ctx.progress.print_message(
            f"batch with file {batch_info['batch'][0]} peak RSS {format_mib(result['peak_rss_bytes']):.0f} MiB"
        )


def parse_args(argv: list[str] | None = None):
    if argv is None:
        argv = sys.argv[1:]
    parser = argparse.ArgumentParser()
    parser.add_argument("--test-list", type=Path)
    parser.add_argument("--changed-tests", type=Path, help="extra test list file; requires --test-list")
    parser.add_argument(
        "--stabilize-tests",
        action="store_true",
        help="rerun selected tests with stabilization logic (fast/slow repetition policy)",
    )
    parser.add_argument("--workers", default=DEFAULT_WORKERS)
    parser.add_argument(
        "--test-config",
        action="append",
        default=[],
        help="path to test config; may be passed multiple times and runs each config independently",
    )
    parser.add_argument(
        "--test-flags",
        default="",
        help="additional flags appended to the unittest binary for listing and execution",
    )
    parser.add_argument("unittest_bin", nargs="?", help=argparse.SUPPRESS)
    parser.add_argument("patterns", nargs="*")
    parser.add_argument(
        "--test-command",
        default="{binary} {flags} --use-colour yes -f {test_list}",
        help="shell command template used to run a test batch; supports {binary}, {flags}, and {test_list}",
    )
    parser.add_argument(
        "--track-runtime",
        type=int,
        nargs="?",
        const=DEFAULT_RUNTIME_THRESHOLD_SECONDS,
        default=None,
    )
    parser.add_argument(
        "--track-rss-memory",
        type=int,
        nargs="?",
        const=DEFAULT_RSS_MEMORY_THRESHOLD_MIB,
        default=None,
        help="print batches whose peak RSS meets or exceeds the threshold in MiB (default: 1024)",
    )
    parser.add_argument("--fail-fast", action="store_true")
    parser.add_argument("--max-failures", type=int)
    parser.add_argument("--retry", type=int, default=0)
    parser.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--batch-timeout", type=float)
    parser.add_argument(
        "--fail-require-skip",
        action="store_true",
        help="fail a batch if unittest output reports skipped tests for `require ...` reasons",
    )
    # Accept options interleaved with positional patterns, e.g.:
    #   run_tests.py bin "[tag]" --fail-fast test/sql/foo.test
    return parser.parse_intermixed_args(argv)


@dataclass(frozen=True)
class InvocationResult:
    returncode: int
    stdout: str
    stderr: str


@dataclass(frozen=True)
class ConfigInvocation:
    label: str
    test_flags: str
    test_config: str | None


@dataclass(frozen=True)
class ConfigRunResult:
    returncode: int
    passed_tests: int
    failed_tests: int
    skipped_tests: int
    elapsed_seconds: float


def build_test_flags(base_flags: str, test_config: str | None):
    if not test_config:
        return base_flags
    config_flag = f"--test-config {shlex.quote(test_config)}"
    return " ".join(flag for flag in [base_flags, config_flag] if flag)


def build_config_invocations(test_configs: list[str], base_flags: str):
    if not test_configs:
        return [ConfigInvocation(label="default", test_flags=base_flags, test_config=None)]
    return [
        ConfigInvocation(
            label=test_config, test_flags=build_test_flags(base_flags, test_config), test_config=test_config
        )
        for test_config in test_configs
    ]


def create_temp_test_list(
    unittest_bin: str,
    test_flags: str,
    patterns: list[str],
    test_list_files: list[Path] | None,
):
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf8", delete=False) as test_file:
        generate_test_list(test_file, unittest_bin, test_flags, patterns, test_list_files)
        return Path(test_file.name)


def run_single_config(
    args,
    unittest_bin: str,
    workers: int,
    retry: int,
    max_retries: int,
    max_failures: int | None,
    batch_size: int,
    test_list_files: list[Path],
    invocation: ConfigInvocation,
    print_config_header: bool,
):
    if stop_requested():
        return ConfigRunResult(returncode=130, passed_tests=0, failed_tests=0, skipped_tests=0, elapsed_seconds=0.0)
    if print_config_header:
        print(f"=== config run: {invocation.label} ===")
    generated_test_list: Path | None = None
    try:
        if args.test_list is not None and len(test_list_files) == 1:
            test_list_path = args.test_list
        else:
            generated_test_list = create_temp_test_list(
                unittest_bin, invocation.test_flags, args.patterns, test_list_files
            )
            test_list_path = generated_test_list
        config = TestRunnerConfig(
            test_list=test_list_path,
            unittest_bin=unittest_bin,
            test_flags=invocation.test_flags,
            patterns=args.patterns,
            test_command=args.test_command,
            workers=workers,
            retry=retry,
            max_retries=max_retries,
            batch_size=batch_size,
            batch_timeout_seconds=args.batch_timeout,
            rss_memory_threshold_mib=args.track_rss_memory,
            runtime_threshold_seconds=args.track_runtime,
            max_failures=max_failures,
            fail_require_skip=args.fail_require_skip,
        )

        tests = load_tests(config.test_list)
        if stop_requested():
            return ConfigRunResult(returncode=130, passed_tests=0, failed_tests=0, skipped_tests=0, elapsed_seconds=0.0)
        if len(tests) == 0:
            print(f"error: no tests selected for config '{invocation.label}'")
            return ConfigRunResult(returncode=1, passed_tests=0, failed_tests=1, skipped_tests=0, elapsed_seconds=0.0)
        stabilization_tests = []
        if args.changed_tests is not None:
            merged_names = {test.name for test in tests}
            base_names = {test.name for test in load_tests(args.test_list)}
            changed_test_names = merged_names - base_names
            added_test_count = len(changed_test_names)
            print(f"added {added_test_count} tests from --changed-tests file to the smoke test run")
            changed_test_name_set = set(changed_test_names)
            stabilization_tests = [test for test in tests if test.name in changed_test_name_set]
        elif args.stabilize_tests:
            stabilization_tests = tests
        computed_batch_size = compute_batch_size(len(tests), config)

        config_values = asdict(config)
        config_values["batch_size"] = computed_batch_size
        config_values.pop("test_list", None)
        config_values.pop("unittest_bin", None)
        config_values.pop("test_command", None)
        config_values = {k: v for k, v in config_values.items() if v is not None and v != "" and v != []}
        config_output = ", ".join(f"{key}={value}" for key, value in config_values.items())
        print(f"config: {config_output}")

        batches = list(chunked(tests, computed_batch_size))
        initial_run_result = run_tests(config, batches, len(tests))
        if initial_run_result.returncode != 0 or not stabilization_tests:
            return initial_run_result

        fast_tests, slow_tests = split_fast_slow_tests(stabilization_tests)
        fast_extra_runs, slow_extra_runs = stabilization_extra_runs(len(stabilization_tests))
        print(
            "stabilizing tests: "
            f"{len(stabilization_tests)} changed/selected tests "
            f"({len(fast_tests)} fast, {len(slow_tests)} slow), "
            f"extra reruns fast={fast_extra_runs}, slow={slow_extra_runs}"
        )

        stabilization_failed = False
        for rerun_idx in range(max(fast_extra_runs, slow_extra_runs)):
            rerun_round = rerun_idx + 1
            if rerun_idx < fast_extra_runs and fast_tests:
                print(f"stabilization rerun {rerun_round}/{fast_extra_runs} for fast tests")
                fast_batches = list(chunked(fast_tests, computed_batch_size))
                fast_result = run_tests(config, fast_batches, len(fast_tests))
                if fast_result.returncode != 0:
                    stabilization_failed = True
            if rerun_idx < slow_extra_runs and slow_tests:
                print(f"stabilization rerun {rerun_round}/{slow_extra_runs} for slow tests")
                slow_batches = list(chunked(slow_tests, computed_batch_size))
                slow_result = run_tests(config, slow_batches, len(slow_tests))
                if slow_result.returncode != 0:
                    stabilization_failed = True
            if stabilization_failed:
                break

        if stabilization_failed:
            print("error: stabilization rerun failure detected")
            return ConfigRunResult(
                returncode=1,
                passed_tests=initial_run_result.passed_tests,
                failed_tests=max(1, initial_run_result.failed_tests),
                skipped_tests=initial_run_result.skipped_tests,
                elapsed_seconds=initial_run_result.elapsed_seconds,
            )

        return initial_run_result
    finally:
        if generated_test_list is not None:
            generated_test_list.unlink(missing_ok=True)


def main(argv: list[str] | None = None):
    enable_line_buffering()
    STOP_REQUESTED.clear()
    previous_sigint_handler = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, signal_stop_requested)
    try:
        return main_impl(argv)
    finally:
        signal.signal(signal.SIGINT, previous_sigint_handler)


def main_impl(argv: list[str] | None = None):
    args = parse_args(argv)
    if args.changed_tests is not None and args.test_list is None:
        print("error: --changed-tests requires --test-list", file=sys.stderr)
        return 1
    if not args.unittest_bin:
        print("error: missing unittest binary", file=sys.stderr)
        return 1

    test_list_files = [path for path in [args.test_list, args.changed_tests] if path is not None]
    config_invocations = build_config_invocations(args.test_config, args.test_flags)
    is_ci = bool(os.environ.get("CI"))
    use_config_groups = is_ci and len(config_invocations) > 1
    max_failures = args.max_failures
    if args.fail_fast:
        max_failures = 1
    retry = max(0, args.retry)
    if retry == 0 and os.environ.get("CI"):
        retry = 2
        print("CI detected, enabling retry=2 per batch")
    max_retries = max(0, args.max_retries)
    workers = resolve_workers(args.workers)
    args.batch_timeout = resolve_batch_timeout(args.batch_timeout, workers)
    unittest_bin = args.unittest_bin
    if os.name == "nt":
        unittest_bin = unittest_bin.replace("/", "\\")
    batch_size = args.batch_size
    failed_configs = []
    if len(config_invocations) > 1:
        print(f"running {len(config_invocations)} configs")
    for invocation in config_invocations:
        if stop_requested():
            print("interrupted")
            return 130
        group_open = False
        if use_config_groups:
            print(f"::group::test config: {invocation.label}")
            group_open = True
        print_config_header = not use_config_groups and not (
            len(config_invocations) == 1 and invocation.label == "default"
        )
        try:
            run_result = run_single_config(
                args,
                unittest_bin,
                workers,
                retry,
                max_retries,
                max_failures,
                batch_size,
                test_list_files,
                invocation,
                print_config_header,
            )
        except Exception as exc:
            print(f"error: {exc}")
            run_result = ConfigRunResult(
                returncode=1, passed_tests=0, failed_tests=1, skipped_tests=0, elapsed_seconds=0.0
            )
        returncode = run_result.returncode
        if group_open:
            print("::endgroup::")
        if returncode in (0, 1):
            if run_result.failed_tests > 0:
                print(
                    "❌ ran tests: "
                    f"{run_result.passed_tests} passed, {run_result.failed_tests} failed, "
                    f"{run_result.skipped_tests} skipped in {run_result.elapsed_seconds:.0f}s"
                )
            else:
                print(
                    "ran tests: "
                    f"{run_result.passed_tests} passed, {run_result.skipped_tests} skipped "
                    f"in {run_result.elapsed_seconds:.0f}s"
                )
        if returncode == 130:
            print("interrupted")
            return 130
        if returncode != 0:
            failed_configs.append(invocation.label)

    if failed_configs:
        if len(config_invocations) == 1:
            return 1
        print(f"error: {len(failed_configs)} config runs failed: {', '.join(failed_configs)}")
        return 1
    if len(config_invocations) > 1:
        print(f"all {len(config_invocations)} config runs passed")
    return 0


def invoke(argv: list[str], cwd: Path | None = None) -> InvocationResult:
    stdout_buffer = StringIO()
    stderr_buffer = StringIO()
    old_cwd = os.getcwd()
    try:
        if cwd is not None:
            os.chdir(cwd)
        with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
            returncode = int(main(argv) or 0)
    finally:
        os.chdir(old_cwd)
    return InvocationResult(returncode=returncode, stdout=stdout_buffer.getvalue(), stderr=stderr_buffer.getvalue())


def run_tests(config: TestRunnerConfig, batches, total_tests: int):
    start = time.monotonic()
    state = BatchRunState()
    progress = DotProgressBar(len(batches))
    total_skipped_tests = 0
    skipped_reason_counts = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=config.workers) as executor:
        future_to_batch = {}
        next_batch_idx = 0
        ctx = RunContext(
            executor=executor,
            config=config,
            state=state,
            future_to_batch=future_to_batch,
            progress=progress,
        )

        if stop_requested():
            return 130
        next_batch_idx = submit_batches(executor, config, batches, future_to_batch, next_batch_idx)

        while future_to_batch:
            if stop_requested():
                state.stop_launching = True
                for future in future_to_batch:
                    future.cancel()
                break
            done, _ = concurrent.futures.wait(
                future_to_batch,
                timeout=0.2,
                return_when=concurrent.futures.FIRST_COMPLETED,
            )
            if not done and stop_requested():
                state.stop_launching = True
                for future in future_to_batch:
                    future.cancel()
                break
            for future in done:
                batch_info = future_to_batch.pop(future)
                result = future.result()
                elapsed = time.monotonic() - batch_info["start"]
                report_batch_metrics(ctx, batch_info, result, elapsed)
                if result["failed"]:
                    if handle_failed_batch(ctx, batch_info, result):
                        continue
                else:
                    attempt_summaries = ctx.state.pop_failed_attempts(batch_info["batch_idx"])
                    if attempt_summaries:
                        ctx.progress.print_message(
                            format_batch_failure(
                                batch_info["batch"],
                                ctx.config,
                                attempt_summaries,
                                recovered=True,
                                retry_count=batch_info["attempt"],
                            )
                        )
                skipped_count, skipped_reasons = extract_skipped_test_output(result["stdout"], result["stderr"])
                total_skipped_tests += skipped_count
                for reason, count in skipped_reasons.items():
                    skipped_reason_counts[reason] = skipped_reason_counts.get(reason, 0) + count
                progress.advance(next_batch_idx - len(future_to_batch))

            if not state.stop_launching and not stop_requested():
                next_batch_idx = submit_batches(executor, config, batches, future_to_batch, next_batch_idx)

    progress.flush_line()
    elapsed = time.monotonic() - start
    if stop_requested():
        return ConfigRunResult(returncode=130, passed_tests=0, failed_tests=0, skipped_tests=0, elapsed_seconds=elapsed)
    exit_code = 0
    if state.failed_count:
        exit_code = 1
    elif total_skipped_tests:
        print(f"all tests passed in {elapsed:.0f}s ({total_skipped_tests} skipped tests)")
    else:
        print(f"all tests passed in {elapsed:.0f}s")
    if skipped_reason_counts:
        print()
        print("Skipped tests for the following reasons:")
        for reason in sorted(skipped_reason_counts):
            print(f"{reason}: {skipped_reason_counts[reason]}")
    failed_tests = state.failed_count
    passed_tests = max(0, total_tests - failed_tests - total_skipped_tests)
    return ConfigRunResult(
        returncode=exit_code,
        passed_tests=passed_tests,
        failed_tests=failed_tests,
        skipped_tests=total_skipped_tests,
        elapsed_seconds=elapsed,
    )


if __name__ == "__main__":
    raise SystemExit(main())
