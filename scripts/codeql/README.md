# CodeQL

Run CodeQL locally or in CI.

There are two helper scripts:

- `scripts/codeql/build.sh`: create a C++ CodeQL database at `build/codeql/db-cpp`.
- `scripts/codeql/run.sh`: analyse and write outputs to `scripts/codeql/out/`.

## Prerequisites

- `codeql` CLI available. Use `brew install codeql`.
- `make codeql` works locally.

## Build a database

```bash
scripts/codeql/build.sh
```

This recreates `build/codeql/db-cpp`.

## Analyse the database

```bash
scripts/codeql/run.sh
```

Or download the database (`cpp-db`) from a CodeQL CI run, extract the zip, and run:

```bash
scripts/codeql/run.sh ~/Downloads/cpp-db
```

Results are written to `scripts/codeql/out/results.sarif`.
