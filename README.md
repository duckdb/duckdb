# DUCKDB Extension 

Adding range encoding bitmap to zonemap in DuckDB


## 1. Build DuckDB

From the project root, run:

```bash
make debug
```

## 2. Run the synthetic mini benchmark

All commands below are run from the root folder of the DuckDB repo.

### 2.1 Run benchmark setup

This creates the benchmark tables and populates them with data:

```bash
build/debug/duckdb bench.db < bench_setup.sql
```

### 2.2 Run benchmark with column imprints OFF

This query run with the column imprints turned off and saves the output to `bench_off.out`:

```bash
build/debug/duckdb bench.db < bench_off.sql | tee bench_off.out
```

### 2.3 Run benchmark with column imprints ON

This query run enables column imprints and the result is saved to `bench_on.out`:

```bash
build/debug/duckdb bench.db < bench_on.sql | tee bench_on.out
```
