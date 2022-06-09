# Julia C-API wrapper for DuckDB

## Installation

```julia
pkg> add DuckDB

julia> using DuckDB
```

## Basics

```julia
con = DuckDB.connect(":memory:")

res = DuckDB.execute(con,"CREATE TABLE integers(date DATE, jcol INTEGER)")

res = DuckDB.execute(con,"INSERT INTO integers VALUES ('2021-09-27', 4), ('2021-09-28', 6), ('2021-09-29', 8)")

res = DuckDB.execute(con, "SELECT * FROM integers")

DuckDB.toDataFrame(res)


# or

DuckDB.toDataFrame(con, "SELECT * FROM integers")

DuckDB.disconnect(con)
```
