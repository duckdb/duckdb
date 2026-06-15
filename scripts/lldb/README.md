# LLDB Helpers

This directory contains small LLDB helpers for DuckDB development.

## `duckdb_sqllogictest.py`

Adds `sql_`-prefixed LLDB commands for sqllogictest-aware debugging.

It assumes the active sqllogictest is running on thread 1 and uses
`query_break` as the hook before each sqllogictest statement/query.

### One-off usage

```lldb
command script import <duckdb repository root>/scripts/lldb/duckdb_sqllogictest.py
b <some breakpoint>
r
sql_current_statement
sql_next_statement
sql_next_matching_statement --kind query
sql_next_matching_statement --connection con2
sql_watch_statement --file test/sql/join --loop i=3
sql_watch_statement --connection con2
sql_next_watch
sql_list_watches
sql_delete_watch 5
```

### Suggested `~/.lldbinit`

```lldb
command script import <duckdb repository root>/scripts/lldb/duckdb_sqllogictest.py
```

### Commands

- `sql_current_statement`
  - prints the sqllogictest file, line, kind, connection, SQL, and active loop values
- `sql_next_statement`
  - continues until the next sqllogictest statement/query
- `sql_next_matching_statement`
  - continues until the next sqllogictest statement/query matching the supplied filters
- `sql_watch_statement`
  - installs a persistent sqllogictest-aware watch rule
- `sql_next_watch`
  - continues until one installed watch rule matches
- `sql_list_watches`
  - lists installed watch rules
- `sql_delete_watch <id>`
  - removes one installed watch rule

### Filters

`sql_next_matching_statement` and `sql_watch_statement` support:

- `--file <substring>`
- `--line <n>`
- `--line-min <n>`
- `--line-max <n>`
- `--kind query|statement`
- `--connection <name>`
- `--loop <name>`
- `--loop <name>=<value>`

### Watch Behavior

`sql_watch_statement` defines persistent watch rules, but normal `c` does not stop on
those rules by itself.

Use `sql_next_watch` to temporarily arm the installed watch rules while
auto-continuing your other breakpoints until the first watch match is reached.

You can optionally pass a watch id to only arm one installed watch:

```lldb
sql_next_watch
sql_next_watch 5
```
