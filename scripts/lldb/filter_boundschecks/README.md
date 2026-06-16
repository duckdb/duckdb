# LLDB Step-Avoid Rules For Checked Wrappers

This helper augments LLDB's `target.process.thread.step-avoid-regexp` so
`step` and `thread step-in` skip small DuckDB wrapper/container frames that are
usually noise while debugging.

The bundled patterns cover helper functions on:

- `duckdb::optional_ptr`
- `duckdb::unique_ptr`
- `duckdb::shared_ptr`
- `duckdb::vector`

Examples include `operator*`, `operator->`, `operator[]`, `get`, and
DuckDB-specific bounds or validity checks.

## Intended Usage

Import the script:

```lldb
command script import <duckdb repository root>/scripts/lldb/filter_boundschecks/filter_checks.py
```

Importing the script immediately enables the DuckDB step-avoid additions for the
current LLDB session.

## Commands

The script registers:

- `duckdb-step-avoid-enable`
- `duckdb-step-avoid-disable`
- `duckdb-step-avoid-show`

Typical flow:

```lldb
duckdb-step-avoid-show
duckdb-step-avoid-enable
duckdb-step-avoid-disable
```

## Behavior

- `duckdb-step-avoid-enable` appends the DuckDB patterns to the current
  `target.process.thread.step-avoid-regexp`
- `duckdb-step-avoid-disable` restores the regexp value that was present before
  the helper first enabled itself
- `duckdb-step-avoid-show` prints both the current LLDB regexp and the DuckDB
  additions managed by the script

The helper remembers the original regexp for the lifetime of the imported
module, so disabling returns LLDB to the prior setting instead of clearing the
value outright.

## Caveat

The script currently registers Python callbacks under the module name
`duckdb_step_avoid`, while the checked-in filename is `filter_checks.py`. If you
import the file exactly as it exists in-tree and LLDB cannot resolve the command
callbacks, the registration strings in the script will need to match the
imported module name.
