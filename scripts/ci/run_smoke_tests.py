#!/usr/bin/env python3
import concurrent.futures
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path

SMOKE_LIST = Path("test/smoke_tests.list")
GROUP_SIZE = 20


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


def run_batch(unittest_bin: str, batch):
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf8", delete=True) as batch_file:
        batch_file.write("\n".join(batch))
        batch_file.write("\n")
        batch_file.flush()
        proc = subprocess.run(
            [unittest_bin, "--use-colour", "yes", "-f", batch_file.name],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    if proc.returncode == 0:
        return None

    rerun_cmd = (
        "printf '%s\\n' "
        + " ".join(shlex.quote(test) for test in batch)
        + " > /tmp/duckdb_smoke_batch.txt && "
        + f"{shlex.quote(unittest_bin)} --use-colour yes -f /tmp/duckdb_smoke_batch.txt"
    )
    parts = [
        "### failed test batch ###",
        "",
        "=== stdout ===",
        proc.stdout.strip(),
        "",
        "=== stderr ===",
        proc.stderr.strip(),
        "",
        "=== reproduce ===",
        rerun_cmd,
        "",
    ]
    return "\n".join(parts)


def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <unittest_bin> <workers>", file=sys.stderr)
        return 1

    unittest_bin = sys.argv[1]
    workers = max(1, int(sys.argv[2]))

    tests = load_tests(SMOKE_LIST)

    print(f"found {len(tests)} tests, group_size={GROUP_SIZE}, workers={workers}")

    batches = list(chunked(tests, GROUP_SIZE))
    failed_count = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(run_batch, unittest_bin, batch) for batch in batches]
        for future in concurrent.futures.as_completed(futures):
            log = future.result()
            if log is not None:
                failed_count += 1
                print(log, end="")
                print("========================")

    if failed_count:
        print(f"error: found {failed_count} test failures")
        return 1

    print("all smoke tests passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
