DuckDB implementation of the queries from the [LDBC Social Network Benchmark](https://arxiv.org/abs/2001.02299).

Download the data:

```bash
python download-benchmark-data.py
```

Initialize the schema:

```bash
cat schema.sql | duckdb ldbc.duckdb
```

Load the data:

```bash
sed "s|PATHVAR|`pwd`/sf0.1|" snb-load.sql | duckdb ldbc.duckdb
```
