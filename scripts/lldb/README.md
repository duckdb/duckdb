# LLDB Helpers

This directory contains small LLDB helpers for DuckDB development.

## `duckdb_sqllogictest.py`

Adds a `current_statement` command, to get the active sqllogictest location and query.

It assumes the active sqllogictest is running on thread 1.

Adds a `next_statement` command, to continue until the next sqllogictest statement.

### One-off usage

```lldb
command script import <duckdb repository root>/scripts/lldb/duckdb_sqllogictest.py
b <some breakpoint>
r
current_statement
next_statement
```

### Suggested `~/.lldbinit`

```lldb
command script import <duckdb repository root>/scripts/lldb/duckdb_sqllogictest.py
```

`current_statement` prints:

- the sqllogictest file and line number
- the SQL text currently being executed

`next_statement`:

- continues execution until the next sqllogictest statement on thread 1
- auto-continues ordinary breakpoints until then
