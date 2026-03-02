#!/usr/bin/env python3
import argparse
import concurrent.futures
import shlex
import subprocess
import tempfile
import time
from pathlib import Path

SMOKE_LIST = Path("test/smoke_tests.list")
BATCH_SIZE = 20
BATCH_TIMEOUT_SECONDS = 5 * 60


def chunked(items, n):
    for i in range(0, len(items), n):
        yield items[i : i + n]


def load_tests(path: Path):
    tests = []
    with path.open("r", encoding="utf8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            tests.append(line)
    return tests


def format_batch_failure(batch_idx: int, batch, unittest_bin: str, stdout: str, stderr: str, message: str | None = None):
    rerun_cmd = (
        "printf '%s\\n' "
        + " ".join(shlex.quote(test) for test in batch)
        + " > /tmp/duckdb_smoke_batch.txt && "
        + f"{shlex.quote(unittest_bin)} --use-colour yes -f /tmp/duckdb_smoke_batch.txt"
    )
    parts = [f"### failed test batch {batch_idx} ###", ""]
    if message is not None:
        parts.extend([message, ""])
    parts.extend(
        [
            "=== stdout ===",
            stdout.strip(),
            "",
            "=== stderr ===",
            stderr.strip(),
            "",
            "=== reproduce ===",
            rerun_cmd,
            "",
        ]
    )
    return "\n".join(parts)


def run_batch(unittest_bin: str, batch):
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf8", delete=True) as batch_file:
        batch_file.write("\n".join(batch))
        batch_file.write("\n")
        batch_file.flush()
        try:
            proc = subprocess.run(
                [unittest_bin, "--use-colour", "yes", "-f", batch_file.name],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=BATCH_TIMEOUT_SECONDS,
            )
        except subprocess.TimeoutExpired as exc:
            return {
                "stdout": exc.stdout or "",
                "stderr": exc.stderr or "",
                "message": f"batch timed out after {BATCH_TIMEOUT_SECONDS} seconds",
            }
    if proc.returncode == 0:
        return None

    return {
        "stdout": proc.stdout,
        "stderr": proc.stderr,
        "message": None,
    }


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("unittest_bin")
    parser.add_argument("workers", type=int)
    parser.add_argument("--track-runtime", action="store_true")
    parser.add_argument("--fail-fast", action="store_true")
    parser.add_argument("--max-failures", type=int)
    return parser.parse_args()


def main():
    args = parse_args()
    unittest_bin = args.unittest_bin
    workers = max(1, args.workers)
    max_failures = args.max_failures
    if args.fail_fast:
        max_failures = 1

    tests = load_tests(SMOKE_LIST)

    print(
        f"found {len(tests)} tests, batch_size={BATCH_SIZE}, workers={workers}, "
        f"batch_timeout={BATCH_TIMEOUT_SECONDS}s"
    )

    batches = list(chunked(tests, BATCH_SIZE))
    failed_count = 0
    stop_launching = False

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_batch = {}
        next_batch_idx = 0

        while next_batch_idx < len(batches) and len(future_to_batch) < workers:
            batch = batches[next_batch_idx]
            future = executor.submit(run_batch, unittest_bin, batch)
            future_to_batch[future] = {
                "batch_idx": next_batch_idx,
                "batch": batch,
                "start": time.monotonic(),
            }
            next_batch_idx += 1

        while future_to_batch:
            done, _ = concurrent.futures.wait(
                future_to_batch,
                return_when=concurrent.futures.FIRST_COMPLETED,
            )
            for future in done:
                batch_info = future_to_batch.pop(future)
                result = future.result()
                elapsed = time.monotonic() - batch_info["start"]
                if args.track_runtime:
                    print(f"batch {batch_info['batch_idx']} completed in {elapsed:.2f}s")
                if result is not None:
                    if max_failures is not None and failed_count + 1 >= max_failures:
                        stop_launching = True
                    failed_count += 1
                    print(
                        format_batch_failure(
                            batch_info["batch_idx"],
                            batch_info["batch"],
                            unittest_bin,
                            result["stdout"],
                            result["stderr"],
                            result["message"],
                        ),
                        end="",
                    )
                    print("========================")

            while not stop_launching and next_batch_idx < len(batches) and len(future_to_batch) < workers:
                batch = batches[next_batch_idx]
                future = executor.submit(
                    run_batch,
                    unittest_bin,
                    batch,
                )
                future_to_batch[future] = {
                    "batch_idx": next_batch_idx,
                    "batch": batch,
                    "start": time.monotonic(),
                }
                next_batch_idx += 1

    if failed_count:
        print(f"error: found {failed_count} test batch failures")
        return 1

    print("all smoke tests passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
