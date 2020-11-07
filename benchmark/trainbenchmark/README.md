# Train Benchmark queries

For the benchmark specification see the paper [The Train Benchmark: cross-technology performance evaluation of continuous model queries](https://link.springer.com/10.1007/s10270-016-0571-8) (Softw. Syst. Model. 2018)

## Usage

### Loading the data

The test data is from the benchmark's "Repair" scenario with data set size (scale factor) 1.

```bash
cat schema.sql | duckdb tb.duckdb
TB_DATA_DIR=sf1
sed -i.bkp "s|\\N||g" ${TB_DATA_DIR}/Route.csv
sed "s/PATHVAR/${TB_DATA_DIR}/g" tb-load.sql | duckdb tb.duckdb
```

### Queries

```bash
cat queries/connectedsegments.sql | duckdb tb.duckdb
cat queries/routesensor.sql | duckdb tb.duckdb
cat queries/poslength.sql | duckdb tb.duckdb
cat queries/semaphoreneighbor.sql | duckdb tb.duckdb
cat queries/switchmonitored.sql | duckdb tb.duckdb
cat queries/switchset.sql | duckdb tb.duckdb
```
