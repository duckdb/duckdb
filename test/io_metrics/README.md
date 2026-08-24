# I/O metrics ground-truth test

DuckDB exposes `io.total_bytes_read` / `io.total_bytes_written` profiling metrics.
They are only correct when a `QueryContext` is threaded down to the `FileHandle`
read/write calls. Many code paths drop it, so the reported numbers are often
wrong (under- or over-counted).

This test measures the **real** bytes the process reads/writes at the libc
syscall boundary — independent of DuckDB's own instrumentation — and compares
that ground truth against the reported metrics. Any non-skipped mismatch is a
**hard failure** (no tolerance).

## How it works

1. `io_shim.c` is compiled into a small interposition library that hooks
   `open`/`openat`/`read`/`write`/`pread`/`pwrite`/`close`. For file descriptors
   whose path starts with one of the prefixes in `IOSHIM_INCLUDE`, it sums the
   real bytes transferred and writes the totals to `IOSHIM_OUT` on exit.
   - macOS: injected via `DYLD_INSERT_LIBRARIES` (`__interpose`).
   - Linux/glibc: injected via `LD_PRELOAD` (`dlsym(RTLD_NEXT, …)`).
2. `run_io_metrics_test.py` runs each workload from `test/configs/io_metrics.json`
   under the shim, enables JSON profiling with `tracked_metrics =
   ['io.total_bytes_read', 'io.total_bytes_written']`, and compares the shim's
   ground truth against the reported metric.

Input files for read tests are created in a separate, **un-instrumented**
process so their creation bytes are not counted. External-file tests
(parquet/csv/json) use an in-memory database, so the only file touched during
measurement is the target file — giving an exact 1:1 comparison. Storage tests
open the database file directly (`default_db`) and sum the metric across the
statements.

## Running

```bash
make release                                   # or pass --duckdb PATH
python3 test/io_metrics/run_io_metrics_test.py            # hard-fails on any non-skipped mismatch
python3 test/io_metrics/run_io_metrics_test.py --verbose  # show byte counts for passing tests too
python3 test/io_metrics/run_io_metrics_test.py --filter parquet   # run a subset
python3 test/io_metrics/run_io_metrics_test.py --keep     # keep the scratch dir for inspection
```

Exit code is non-zero if any non-skipped test mismatches. Requires macOS or
Linux/glibc (library interposition must work against the local `duckdb` binary)
and a C compiler (`cc`).

## The skip list

`test/configs/io_metrics.json` is the test config. (It lives alongside the
sqllogictest configs in `test/configs/`, but uses its own schema and is consumed
by this harness, not by `test/run`.) Each entry has `include` (target
path prefixes), optional `prep` (un-instrumented setup), `measure` (instrumented
statements), and `check` (`read` and/or `write`).

An entry with a `skip` field is a **known-wrong metric** — one of the
QueryContext-not-passed bugs. The `skip` string explains why it is currently
wrong (it starts with `FIXME:`). These are reported but do not fail the run.

When such a bug is fixed the metric starts matching ground truth, and the
harness prints `SKIP?<name> now MATCHES ground truth` and counts it as a "stale
skip" so the entry can be removed from the config. This makes the skip list a
living to-do list of the metric bugs.

To turn a fix into a passing regression test: fix the QueryContext threading,
confirm the harness reports the test as a stale skip, then delete its `skip`
field so it becomes a hard check.
