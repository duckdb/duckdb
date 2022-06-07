# Julia C-API wrapper for DuckDB

## Installation

```julia
pkg> add DuckDB

julia> using DuckDB
```

## Basics

```julia
db = DuckDB.open(":memory:")

con = DuckDB.connect(db)

res = DuckDB.execute(con,"CREATE TABLE integers(date DATE, jcol INTEGER);")

res = DuckDB.execute(con,"INSERT INTO integers VALUES ('2021-09-27', 4), ('2021-09-28', 6), ('2021-09-29', 8);")

res = DuckDB.execute(con, "SELECT * FROM integers;")

df = DuckDB.toDataFrame(res)

# or

df = DuckDB.toDataFrame(con, "SELECT * FROM integers;")

res = DuckDB.execute(con, "COPY (SELECT * FROM intergers) TO 'test.parquet' (FORMAT 'parquet');")

res = DuckDB.execute(con, "SELECT * FROM 'test.parquet';")

DuckDB.appendDataFrame(df, con, "integers")

DuckDB.disconnect(con)

DuckDB.close(db)
```
